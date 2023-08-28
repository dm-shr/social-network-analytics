### IMPORTS ###

# file work and time
import io
from datetime import datetime, timedelta

# calculation and data manipulation 
import numpy as np
import pandas as pd
import pandahouse as ph # Connect with clickhouse DB

# plotting
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import seaborn as sns

# report bot-related methods
import telegram

# dag launching
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

### DEFINE PARAMETERS AND CONSTANTS ###

# parameters for Watcher object to initialize
params_feed = {
                'coeffs': [1.5, .6, .6, .5],
                'threshold': 1,
                'n_hits': 3,
                'length_to_check': 1
              }

params_message = {
                'coeffs': [1.4, 1.4],
                'threshold': 1,
                'n_hits': 3,
                'length_to_check': 1
              }

# name of the plots in the report
suptitle =  'Anomalities detection report: metrics for last 4 days'

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'd-shiriaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 15),
}

# interval for dag launching
schedule_interval = timedelta(minutes=15)

# to connect with source database
connection_sim = {
    'host': 'https://clickhouse.lab.karpov.courses',
      'database':'simulator_20230720',
      'user':'student', 
      'password':'dpo_python_2020'
     }

# for how many weeks to get the data, 
# should be optimal for representative interval bounds
n_weeks = 1 

# get feed usage data
feed_query = f"""
SELECT
    toStartOfFifteenMinutes(time) AS time,
    countDistinct(user_id) AS "users_feed  ",
    countIf(action = 'like') AS "likes      ",
    countIf(action = 'view') AS "views       "
FROM simulator_20230720.feed_actions
WHERE time > (today() - {n_weeks} * 7)
GROUP BY time
ORDER BY time ASC
"""

# get message usage data
message_query = f"""
SELECT
    toStartOfFifteenMinutes(time) AS time,
    countDistinct(user_id) AS users_message,
    count() AS messages_sent
FROM simulator_20230720.message_actions
WHERE time > (today() - {n_weeks} * 7)
GROUP BY time
ORDER BY time ASC

"""

feed_query_ctr = f"""
SELECT
    toStartOfHour(time) AS time,
    100 * countIf(action = 'like') / countIf(action = 'view') as "ctr"
FROM simulator_20230720.feed_actions
WHERE time > (today() - {n_weeks} * 7)
GROUP BY time
ORDER BY time ASC
"""


my_token = '6159468274:AAHlSXdxNlB8jm7p7A3U9PLvIHsahKC-vPw' # my api token
bot = telegram.Bot(token=my_token) # получаем доступ

chat_id = -830248324
# chat_id = bot.getUpdates()[0].message.chat.id # get chat id for my private chat with the bot


### DEFINE CLASSES WE WOULD NEED ###

class Extractor:
    """
    Extracts a dataframe, removes the last timestamp value
    """
    @staticmethod
    def extract(query, connection):
        df_cube = ph.read_clickhouse(query=query, connection=connection)
        
        return df_cube.iloc[:-1]
    
    
class Merger:
    """
     merges two dataframes into one:
        feed metrics by 15-min,
        ctr by 1-hour OR
        
        also, adds an 'hour' column showing hour (0-23) corresponding to time column value
    """
    # merge feed dataframes
    @staticmethod
    def merge(feed_df,
              ctr_df,
              time_column='time'
             ):
        
        merged_df = feed_df.merge(ctr_df, how='left', on=time_column)
        
        return merged_df
        
    # add a column indicating hour type
    @staticmethod
    def add_hours_column(df, time_column='time'):
        df['hours'] = df[time_column].dt.hour
        
        return df
    
    # combine merging with hour column addition
    @staticmethod
    def merge_and_add_hours_column(feed_df,
                                  ctr_df,
                                  time_column='time'
                                  ):
        
        merged_df = Merger.merge(feed_df,
                                  ctr_df,
                                  time_column
                                 )
        
        return Merger.add_hours_column(merged_df, time_column)


class Watcher:
    def __init__(self,
                 data,
                 params
                ):
        """
        performs check of a dataframe with metrics for anomalitites
        by finding outliers laying outside the interval:
        q1 - coeff * IQR, 13 + coeff * IQR
        
        args:
        data - dataframe to check
        time_column - column of data with time. should be the first column of the dataframe
        metrics - name of the columns with metrics to be checked. Inferred as all but the first column of a dataframe.
        params - dictionary of arguments:
            coeffs - sensitivity coefficients in a form of a list with order corresponding to the order in metrics.            
            length_to_check - how many last timestamps to check
            n_hits - how many entries with top outliers to return
            threshold - consider an outlier important if its ratio to a bound is out of (100 - threshold, 100 + threshold) interval
        """
        
        self.data = data
        self.time_column = self.data.columns[0]
        self.metrics = list(self.data.columns[1:-1])
        self.length_to_check = params['length_to_check']
        self.coeffs = {metric: coeff for metric, coeff in zip(self.metrics, params['coeffs'])}
        self.n_hits = params['n_hits']
        self.threshold = params['threshold']
        
    
    # find by-hour interval bounds for metrics
    @staticmethod
    def lower_bound(x, coeff):
        q1 = x.quantile(0.25)
        iqr = x.quantile(0.75) - q1
        return q1 - coeff * iqr
    

    @staticmethod
    def upper_bound(x, coeff):
        q3 = x.quantile(0.75)
        iqr = q3 - x.quantile(0.25)
        return q3 + coeff * iqr
    
    
    # function that creates columns with upper and lower
    # interval bounds using corresponding 'coeff' value
    def aggregate_functions(self, metric):
            
            funcs =  [
        ('lower_bound', lambda x: Watcher.lower_bound(x, self.coeffs[metric])),
        ('upper_bound', lambda x: Watcher.upper_bound(x, self.coeffs[metric]))
    ]
            return funcs
        
    
     # make columns with the bounds for each metrics    
    def make_bounds_df(self):        
        # make a dict: metric: functions where for each metric, corresponding 'coeff' is used
        aggregate_dict = {metric: self.aggregate_functions(metric) for metric in self.metrics}

        # aggregate and apply the functions to find borders for each hour
        bounds_df = self.data.groupby('hours').agg(aggregate_dict).reset_index()
        
        # make a list for new column names and add 'hours' column name to it
        aggregated_column_names = ['hours']

        # since bounds_df column names have multiindex, let's combine them into single index names
        for column in bounds_df.columns[1:]:
            aggregated_column_names.append('_'.join(column))

        bounds_df.columns = aggregated_column_names

        # start merging function
        return self.merge_with_bounds(bounds_df)
    
    
    # does a join of the bounds dataframe to the original dataframe
    def merge_with_bounds(self, bounds_df):

        merged_df = self.data.merge(bounds_df, 
                         on='hours', 
                         how='left'
                        )

        # start final function that finds outliers, and filters top outliers
        return self.label_and_sort_outliers(merged_df)
    
        
    # for a particular metric column, create is_outlier column
    # and fill its entries for outliers with ratios to either left or right border
    # of the detection interval.
    @staticmethod
    def make_is_outlier_column(merged_df_tail, metric, outlier_cols, ratio_cols, report_cols):
        # initialize column names we need
        lower_bound_col = f'{metric}_lower_bound'
        upper_bound_col = f'{metric}_upper_bound'
        outlier_col = f'{metric}_is_outlier'
        ratio_col = f'{metric}_outlier_ratio (%)'
        report_col = metric.replace('_', ' ')

        # fill column names lists
        outlier_cols.append(outlier_col)
        ratio_cols.append(ratio_col)
        report_cols.append(report_col)

        merged_df_tail.loc[:, ratio_col] = np.nan  # Initialize the new ratio column

        # get a series where the entries are outliers for the metric
        merged_df_tail[outlier_col] = ~merged_df_tail[metric].between(merged_df_tail[lower_bound_col], merged_df_tail[upper_bound_col])

        # make a masks for lower and upper outlier
        # to get entries below the lower and above the upper borders
        mask_lower_outlier = merged_df_tail[metric] < merged_df_tail[lower_bound_col]
        mask_upper_outlier = merged_df_tail[metric] > merged_df_tail[upper_bound_col]

        # Calculate the ratio for each outlier entry
        merged_df_tail.loc[mask_lower_outlier, ratio_col] = merged_df_tail.loc[mask_lower_outlier, metric]\
        .mul(100).div(merged_df_tail[lower_bound_col])
        merged_df_tail.loc[mask_upper_outlier, ratio_col] = merged_df_tail.loc[mask_upper_outlier, metric]\
        .mul(100).div(merged_df_tail[upper_bound_col]) 
        
        merged_df_tail[report_col] = merged_df_tail[ratio_col].round(0).fillna('_').astype(str)\
                                    .str.cat(merged_df_tail[metric].astype(str),
                                             sep=' - ',
                                             na_rep=''
                                             )\
                                    .replace(r'.*_', np.nan, regex=True)
    
    
    def label_and_sort_outliers(self, merged_df):
        """
        For the merged dataframe with the bounds columns,
        
        a) shortens the dataframe to look for anomalities within last entries ('self.length_to_check') only;
        b) makes an is_outlier column for each metric;
        c) makes ToPlot object for each metric and whole timescale to get plots for future use;
        d) filters the shortened dataframe to get only outlier entries deviating more than threshold %
            from interval bounds;
        e) sorts the entries to get top-outliers first;
        f) leaves only top 'n_hits' entries, and leaves only time and ratio columns;
        g) finally, returns its Watcher object for future use by Reporter object.
        
        """
        
        # make a list of names of outlier, ratio columns,
        # and final columns having concatenated ratios and raw values
        outlier_cols = []
        self.ratio_cols = []
        self.report_cols = []
        
        # variable showing if there are outliers to report
        self.to_report = False
        
        # list for plots with the metrics - they are plotted at the report stage
        # in case Watcher instance returns to_report = True
        self.to_plot_objects = []
        
        # shorten - get last rows of the table
        merged_df_tail = merged_df.tail(self.length_to_check).copy()
            
        # iterate over the columns, make is_outlier column and create ToPlot object
        for metric in self.metrics:
            
            Watcher.make_is_outlier_column(merged_df_tail, metric, outlier_cols, self.ratio_cols, self.report_cols)
            
            # merged_df_tail[
            
            to_plot_object = ToPlot( 
                                 data=merged_df.iloc[-4*24*4:], 
                                 ylabel=metric,
                                 title=metric,
                                 xlabel='',
                                 x=self.time_column,
                                 y=metric
                                )
            
            self.to_plot_objects.append(to_plot_object)
            
        # drop the rows where all the metrics are within the limits
        filtered_result_with_outliers = merged_df_tail[merged_df_tail[outlier_cols].any(axis=1)]
        
        # Filter rows where at least one entry deviates from 100% by more than 'self.threshold'%
        filtered_result_with_outliers = filtered_result_with_outliers\
        [(abs(filtered_result_with_outliers.loc[:, self.ratio_cols] - 100) > self.threshold)\
         .any(axis=1)]
        
        # if no rows with outliers outside the threshold, return
        if filtered_result_with_outliers.shape[0] == 0:
            self.report_df = None
            return self
        
        # since we will report, set to True
        self.to_report = True        
        
        # Calculate the maximum value between ratio_cols
        filtered_result_with_outliers['MaxValue'] = np.abs(filtered_result_with_outliers[self.ratio_cols].sub(100)).max(axis=1)

        # Sort the DataFrame based on the MaxValue column in descending order
        report_df = filtered_result_with_outliers.sort_values(by='MaxValue', ascending=False)

        # Drop the temporary MaxValue column
        report_df.drop(columns=['MaxValue'], inplace=True)
        
        # format the time of the time column for better report view        
        report_df[self.time_column] = report_df[self.time_column].dt.strftime('%Y-%m-%d %H:%M')

        # select top hits, drop value and bound columns, drop the index
        report_df = report_df\
                        .head(self.n_hits)\
                        .loc[:, [self.time_column] + self.report_cols]\
                        .reset_index(drop=True)\
                        .dropna(axis=1, how='all')\
                        .rename(columns={self.time_column: self.time_column + ' ' * 12})     
                        
        # make a df attribute and return itself
        self.report_df = report_df
        
        return self                
    
    
    # main function launching check-up process
    def watch(self):
        return self.make_bounds_df()
    

class ToPlot:
    """
    prepares a plot object from the given dataframe
    and plots it onto a given grid using plot() method
    """    
    def __init__(self, 
                 data, 
                 ylabel,
                 title,
                 x,
                 y,
                 xlabel=''
                ):
        
        self.data = data
        self.ylabel = ylabel
        self.title = title
        self.xlabel = xlabel
        self.x = x
        self.y = y
        
        
    def __repr__(self):
        return str((self.title, self.ylabel, self.xlabel))
    
        
    # plots formatted plot onto a given grid
    def plot(self, ax, scale=1):
        
        self.lineplot(ax, scale)  
        self.format_plot(ax, scale)
        
    
    # formats a plot
    def format_plot(self, ax, tick_rotation, scale=1):
        
        fontsize = 12 * scale**(0.5)
        labelsize = 11 * scale**(0.5)
        
        ax.set_title(self.title, fontsize=fontsize)
        ax.set_xlabel(self.xlabel, fontsize=fontsize)
        ax.set_ylabel(self.ylabel, fontsize=fontsize)

        # Set the tick labels

        ax.tick_params(axis='x', rotation=45, labelsize=labelsize)
        ax.tick_params(axis='y', rotation=0, labelsize=labelsize)       
        
        
    # makes lineplot
    def lineplot(self, ax, scale=1):
        """
        Plots a lineplot for the given dataframe (table) on the given axis (ax)
        """
        
        # Plot the line plot
        sns.lineplot(
            data=self.data,
            x=self.x, 
            y=self.y, 
            ax=ax,
            linewidth=2*scale**(.4),
        )        
        
        # Fill the area between the bounds using the specified 'ax' object
        ax.fill_between(self.data[self.x], 
                        self.data[f'{self.y}_lower_bound'], 
                        self.data[f'{self.y}_upper_bound'], 
                        alpha=0.5, 
                        edgecolor='red', 
                        linewidth=2*scale**(.4)
                       )

        date_format = plt.matplotlib.dates.DateFormatter('%b %d, %H:%M')
        ax.xaxis.set_major_formatter(date_format)
                
        
class Reporter:
    """
    makes a final report and sends it via TG
    """
    def __init__(self,
                 watcher,
                 chat_id,
                 bot,
                 suptitle=None,
                 scale=.3
                ):
        
        # do not initialize all the attributes if does not report
        if not watcher.to_report:
            self.to_report = False
            return
        
        self.to_report = True
        self.watcher = watcher
        self.chat_id = chat_id
        self.bot = bot
        self.suptitle = suptitle
        self.scale = scale
        
        # parameters for plot scaling
        self.n_cols = 2
        self.n_rows = int( np.ceil( len ( self.watcher.to_plot_objects ) / 2 ) )
        self.figsize = (12, 4 * self.n_rows)        
        
    
    # sends plots built by make_plots() method
    def send_plots(self):
        self.make_plots()
        plot_object = io.BytesIO()
        plt.savefig(plot_object, dpi=150)
        plot_object.seek(0)
        plot_object.name = 'report_plot.png'
        plt.close()
        self.bot.sendPhoto(chat_id=self.chat_id, photo=plot_object)
        
        
    # makes plots from to_plot_objects attribute of a watcher object attribute    
    def make_plots(self):
        # make a canvas, set sizes
        fig, axs = plt.subplots(self.n_rows, self.n_cols, figsize=self.figsize)

        # make a fig title
        fig.suptitle(self.suptitle, fontsize=14)

        for to_plot_obj, ax in zip(self.watcher.to_plot_objects, axs.reshape(-1)):
            to_plot_obj.plot(ax=ax, scale=self.scale)
            
        plt.tight_layout()
        
        
    # sends a text report
    def send_report_text(self):
        
        # define current time with 3 hr shift to account for time zones
        current_datetime = datetime.now() + timedelta(hours=3)
        
        # make a template for a report
        header = \
        f"<pre>Anomaly detection report for {current_datetime.strftime('%Y-%m-%d %H:%M')}:\n{'-'*30}\n"\
        f"Last {int(self.watcher.length_to_check * 15)} minutes were checked.\n"\
        f"top-{self.watcher.n_hits} outliers are shown.\n"\
        f"sensitivity coeffs = {self.watcher.coeffs}.\n"\
        f"values in the report indicate:\n"\
        f"value / lower interval bound ratio for low outliers (&lt 100),\n"\
        f"value / upper interal bound ratio for upper ones (&gt 100).\n"\
        f"bounds are Q1 - (sensitivity coeff) * IQR, Q3 + (sensitivity coeff) * IQR.\n"\
        f"raw metric values are shown after dash.\n"\
        f"system reports only outliers lying {self.watcher.threshold}% or more outside the bounds.\n"\
        f"N/A indicates no deviation of the metric.\n{'-'*30}\n"        
        
        table = self.watcher.report_df.to_csv(sep='|', index=False).replace('|',  ' '*2)
        
        text = header + table + '</pre>'        
        
        self.bot.sendMessage(chat_id=self.chat_id, 
                            text=text, 
                            parse_mode='HTML'
                           )   
        
        
    # main function launching the report
    def report(self):
        # if not initialized properly, do not report (when no outliers found)
        if not self.to_report:
            print('Nothing to report, silent mode')
            return
        
        self.send_report_text()
        self.send_plots()

        

### DAG CREATION ###
# create a dag
@dag(
    default_args=default_args, 
    schedule_interval=schedule_interval, 
    catchup=False
)
def dag_sim_shiriaev_task_10_1(): 
    
    @task
    def extract(query, connection):
        df = Extractor.extract(query, connection)

        return df


    @task
    def merge_and_add_hours_column(feed_df, ctr_df):
        return Merger.merge_and_add_hours_column(feed_df, ctr_df)


    @task
    def add_hours_column(message_df):
        return Merger.add_hours_column(message_df)


    @task
    def watch(data, params):
        watcher = Watcher(data,
                          params
                         )                     

        return watcher.watch()


    @task
    def report(watcher, chat_id, bot, suptitle=None, scale=0.3):

        reporter = Reporter(watcher,
                         chat_id,
                         bot,
                         suptitle,
                    )

        reporter.report()
 

    ### SET THE TASK ORDER ###

    # get the data
    feed_df = extract(feed_query, connection_sim)
    message_df = extract(message_query, connection_sim)
    ctr_df = extract(feed_query_ctr, connection_sim)

    # transform and pre-plot the data
    merged_feed = merge_and_add_hours_column(feed_df, ctr_df)
    message_with_hours_df = add_hours_column(message_df)

    watcher_feed = watch(data=merged_feed, params=params_feed)
    watcher_message = watch(data=message_with_hours_df, params=params_message)

    # send a report plot
    report(watcher=watcher_feed, chat_id=chat_id, bot=bot, suptitle=suptitle, scale=0.3)
    report(watcher=watcher_message, chat_id=chat_id, bot=bot, suptitle=suptitle, scale=0.3)

### LAUCH THE DAG ###
dag_sim_shiriaev_task_10_1 = dag_sim_shiriaev_task_10_1()
