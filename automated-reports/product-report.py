### DESCRIPTION ###

# Here, we set up an automated reporting system using Airflow.
# The system performs an ETL task and reports via corporate messenger chat 
# (Telegram in this case).
# The report includes key metrics over past 30 days 
# which describe the development of our product:

# feed-messenger (shared) DAU, 
# CTR, 
# views per user, 
# % of ads-sourced traffic, 
# feed users by retention,
# users demographics

# An example of the report is shown in the file 'product-report.jpg'
 
### IMPORTS ###

# hide database access passwords
import os
from dotenv import load_dotenv

# file work and time
import io
from datetime import datetime, timedelta

# calculation and data manipulation 
import pandas as pd
import pandahouse as ph # Connect with clickhouse DB

# plotting
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import seaborn as sns

# report bot-related methods
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


### DEFINE PARAMETERS AND CONSTANTS ###

# Load environment variables (database password, telegram bot token and chat ID) from .env file
load_dotenv()

# introduce the secret variable to connect to the db
db_pwd = os.environ.get('db_pwd')
bot_token = os.environ.get('bot_token')
chat_id = os.environ.get('chat_id')

# default arguments to be used in our tasks
default_args = {
    'owner': 'd-shiriaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 15),
}

# DAG launching interval, in cron format
schedule_interval = '00 11 * * *'

# to connect with source database
connection_sim = {
    'host': 'https://clickhouse.lab.karpov.courses',
      'database':'simulator_20230720',
      'user':'student', 
      'password': db_pwd
     }

# query to get shared usage data for the report
shared_query = """
WITH cte_feed AS (

SELECT
    toDate(time) AS day,
    user_id,
    countIf(action = 'like') AS likes,
    countIf(action = 'view') AS views,
    age,
    gender,
    os,
    source
FROM simulator_20230720.feed_actions
WHERE toDate(time) BETWEEN (today() - 30) AND today() - 1
GROUP BY 
    toDate(time),
    user_id,
    age,
    gender,
    os,
    source
    
),

cte_message AS (

SELECT
    toDate(time) AS day,
    user_id,
    count() AS messages_sent,
    age,
    gender,
    os,
    source
FROM simulator_20230720.message_actions
WHERE toDate(time) BETWEEN (today() - 30) AND today() - 1
GROUP BY 
    toDate(time) AS day,
    user_id,
    age,
    gender,
    os,
    source

)

SELECT 
    t1.day,
    toString(t1.user_id) AS user_id,
    likes,
    views,
    messages_sent,
    t1.age,
    t1.gender,
    t1.os,
    t1.source
FROM 
cte_feed t1 JOIN cte_message t2 
ON t1.day = t2.day AND t1.user_id = t2.user_id 
"""

# get feed usage data
feed_query = """
SELECT
    toDate(time) AS day,
    toString(user_id) AS user_id,
    countIf(action = 'like') AS likes,
    countIf(action = 'view') AS views,
    age,
    gender,
    os,
    source
FROM simulator_20230720.feed_actions
WHERE toDate(time) BETWEEN (today() - 30) AND today() - 1
GROUP BY 
    toDate(time),
    user_id,
    age,
    gender,
    os,
    source
"""

# get message usage data
message_query = """
SELECT
    toDate(time) AS day,
    toString(user_id) AS user_id,
    count() AS messages_sent,
    age,
    gender,
    os,
    source
FROM simulator_20230720.message_actions
WHERE toDate(time) BETWEEN (today() - 30) AND today() - 1
GROUP BY 
    toDate(time) AS day,
    user_id,
    age,
    gender,
    os,
    source
"""

# get retention data:
# for each week we have amount of:
# 1. new users;
# 2. users active this and previous week;
# 3. users active previous, but not this week.
retention_query = '''
-- table where for every unique pair user-active week, 
-- shows if this user joined that week, was active last week, and was active next week
WITH weeks_labeled AS (
SELECT 
user_id, 
week, 
all_weeks,
start_week,
if(has(all_weeks, week - 7), 1, 0) AS is_previous_week,
if(has(all_weeks, week + 7), 1, 0) AS is_next_week

FROM

-- table showing all user activity weeks aggregated into an array
(SELECT DISTINCT user_id, 
toStartOfWeek(time) AS week, 
groupUniqArray(toStartOfWeek(time)) OVER (PARTITION BY user_id) AS all_weeks,
min(toStartOfWeek(time)) OVER (PARTITION BY user_id) AS start_week
FROM simulator_20230720.feed_actions) t1
)

SELECT week,
new_users,
retained_users,
lost_users

FROM 
-- table having counts for new and retained users by week
(SELECT 
week, 
countIf(week = start_week) AS new_users,
countIf(is_previous_week = 1) AS retained_users
FROM weeks_labeled
GROUP BY week
) with_retained

JOIN 

-- table having counts for users who didn`t come from last week.
-- basically, weeks are shifted one week up to align 
-- with is_next_week column to indicate how many users did not come ("gone")
(SELECT 
week + 7 AS to_next_week,
countIf(is_next_week = 0) * (-1) AS lost_users
FROM weeks_labeled
GROUP BY to_next_week
) with_gone

ON (with_retained.week = with_gone.to_next_week)
WHERE week BETWEEN toStartOfWeek(today()) - 7 * 8 AND toStartOfWeek(today()) - 7
'''


bot = telegram.Bot(token=bot_token) # access the bot


### DEFINE CLASSES WE WOULD NEED ###

# This classes are used to write ETL tasks
# to avoid repetitive actions and allow more readable code

class Extractor:
    """
    Extracts a dataframe and adds a column for product type if needed
    """
    # extract a df given the query and connection details
    @staticmethod
    def extract(query, connection):
        df_cube = ph.read_clickhouse(query=query, connection=connection)
        return df_cube
    
    # add a product type column
    @staticmethod
    def add_column(df_cube, product_type):
        df_cube['product'] = product_type
        return df_cube
    

class Transformer:
    """
    manipulates dataframe(s) to make them suitable for further plotting
    """
    # make df for plot 1 - dau by os
    @staticmethod
    def shared_dau_os_maker(shared_df):
        # group users by date and os type and count them
        return shared_df.groupby(['day', 'os']).count().reset_index()
    

    # make dfs for plots 2 and 3 - ctr by product and views/users by product
    @staticmethod
    def ctr_df_maker(shared_df, feed_df):
        # group shared users by date and count total CTR
        shared_ctr_df = shared_df.groupby(['day'])\
            .agg({'user_id': 'count', 'views': 'sum', 'likes': 'sum', 'product': 'min'})
        shared_ctr_df['ctr'] = shared_ctr_df.likes.mul(100).div(shared_ctr_df.views)
        
        # same for feed users
        feed_ctr_df = feed_df.groupby(['day'])\
            .agg({'user_id': 'count', 'views': 'sum', 'likes': 'sum', 'product': 'min'})
        feed_ctr_df['ctr'] = feed_ctr_df.likes.mul(100).div(feed_ctr_df.views)
        
        # make a merged CTR dataframe and make views per user column
        ctr_df = pd.concat([shared_ctr_df, feed_ctr_df])
        ctr_df['views per active user'] = ctr_df.views.div(ctr_df.user_id)
        
        return ctr_df.reset_index()
    

    # make df for plot 4 - percentage of ads-sourced users by product
    @staticmethod
    def ads_percentage_maker(dfs, products):
        # an array to be filled with transformed dataframes
        processed_dfs = []

        for df, product in zip(dfs, products):
            # group by date, source, count the users, transform the indices
            grouped_df = df.groupby(['day', 'source'], as_index=False).count().loc[:, ['day', 'source', 'user_id']]
            pivot_df = grouped_df.pivot(index='day', columns='source', values='user_id').fillna(pd.NA)
            # make a percentage column
            pivot_df['ads_percentage'] = (pivot_df['ads'] / (pivot_df['ads'] + pivot_df['organic'])) * 100
            # make product type column
            pivot_df['product'] = product

            processed_dfs.append(pivot_df)

        # combine dfs for every product type
        concatenated_df = pd.concat(processed_dfs)

        return concatenated_df.reset_index()
    
    # make df for plot 5 - age distribution by gender and product
    @staticmethod
    def age_distribution_maker(dfs, age_threshold):
        # age_threshold is to cutoff very old users to make plots more clean
        concat_df = pd.concat(dfs)
        return concat_df[concat_df.age < age_threshold]
    
class ToPlot:
    """
    prepares a plot object from the transformed dataframe
    and plots it onto a given grid
    """    
    def __init__(self, 
                 data, 
                 plot_type,
                 ylabel,
                 title,
                 xlabel='',
                 x=None,
                 y=None,
                 hue=None,
                 columns=None,
                 legend_title=None,
                ):
        
        self.data = data
        self.plot_type = plot_type
        self.ylabel = ylabel
        self.title = title
        self.xlabel = xlabel
        self.x = x
        self.y = y
        self.hue = hue
        self.columns = columns
        self.legend_title = legend_title
        
    def __repr__(self):
        return str((self.title, self.ylabel, self.xlabel))
        
    # plots formatted plot onto a given grid
    def plot(self, ax, scale=1):
        
        if self.plot_type == 'lineplot':
            self.lineplot(ax, scale)
            tick_rotation = 45
        elif self.plot_type == 'barplot':
            self.barplot(ax, scale)
            tick_rotation = 45
        elif self.plot_type == 'violinplot':
            self.violinplot(ax, scale)
            tick_rotation = 0
        else:
            raise ValueError("to_plot parameter must be 'lineplot', 'boxplot', or 'violinplot'")     
        
        self.format_plot(ax, tick_rotation, scale)
    
    # formats a plot
    def format_plot(self, ax, tick_rotation, scale=1):
        
        fontsize = 12 * scale**(0.5)
        labelsize = 11 * scale**(0.5)
        
        ax.set_title(self.title, fontsize=fontsize)
        ax.set_xlabel(self.xlabel, fontsize=fontsize)
        ax.set_ylabel(self.ylabel, fontsize=fontsize)

        legend = ax.legend(title=self.legend_title, fontsize=fontsize)
        legend.get_title().set_fontsize(fontsize)
        legend.set_alpha(1)

        # Set the tick labels

        ax.tick_params(axis='x', rotation=tick_rotation, labelsize=labelsize)
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
            hue=self.hue,
            linewidth=2*scale**(0.5),
            marker="o",
            markerfacecolor='white',
            markeredgecolor='black',
            markersize=7*scale
        )        
        
        ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%b %d'))  # Month Day format

    # makes barplot for retention
    def barplot(self, ax, scale=1):
        
        if self.columns:
            df = self.data[columns]
        else:
            df = self.data
        
        df.plot(kind='bar', stacked=True, ax=ax)
        ax.set_xticklabels(self.data.index.date)
    
    # makes violinplot
    def violinplot(self, ax, scale=1):
        # Create the violinplot on the specified ax
        sns.violinplot(data=self.data, 
                       ax=ax,
                       x=self.x, 
                       y=self.y, 
                       hue=self.hue, 
                       split=True, 
                       cut=0, 
                       bw=.08,
                       # scale="count",
                      # inner="stick"
                      )       

class Reporter:
    """
    makes a final plot and sends it via TG
    """
    def __init__(self,
                 to_plot_objects,
                 chat_id,
                 bot,
                 suptitle=None,
                 figsize=(10, 12),
                 scale=.5
                ):
        self.to_plot_objects = to_plot_objects
        self.chat_id = chat_id
        self.bot = bot
        self.suptitle = suptitle
        self.figsize = figsize
        self.scale = scale
        
    
    def send_plots(self):
        self.make_plots()
        plot_object = io.BytesIO()
        plt.savefig(plot_object, dpi=150)
        plot_object.seek(0)
        plot_object.name = 'report_plot.png'
        plt.close()
        self.bot.sendPhoto(chat_id=self.chat_id, photo=plot_object)
        
        
    def make_plots(self):
        # make a canvas, set sizes
        fig, axs = plt.subplots(3, 2, figsize=self.figsize)

        # make a fig title
        fig.suptitle(self.suptitle, fontsize=14)

        for to_plot_obj, ax in zip(self.to_plot_objects, axs.reshape(-1)):
            to_plot_obj.plot(ax=ax, scale=self.scale)
            
        plt.tight_layout()
       

### DAG CREATION ###
# create a dag
@dag(
    default_args=default_args, 
    schedule_interval=schedule_interval, 
    catchup=False
)
def dag_sim_shiriaev_task_9_2(): 
    
    # get a df to work with
    @task
    def extract_df(query, connection, product_type=None):
        df = Extractor.extract(query, connection)
        if product_type:
            df = Extractor.add_column(df, product_type)
        return df

    # toplot for plot 1 - dau by os
    @task
    def transform_dau_os(shared_df):
        data = Transformer.shared_dau_os_maker(shared_df)

        to_plot = ToPlot(
            data=data,
             plot_type='lineplot',
             ylabel='Active Users',
             title='Shared Active Users by OS',
             x='day',
             y='user_id',
             hue='os',
             legend_title='OS type'
                    )
        return to_plot

    # toplot for plot 2/3 - ctr by product / views per user by product
    @task
    def transform_ctr(shared_df, feed_df, plot):

        data = Transformer.ctr_df_maker(shared_df, feed_df)

        if plot == 'ctr':
            to_plot = ToPlot(data=data,
                         plot_type='lineplot',
                         ylabel='CTR (%)',
                         title='Daily CTR in feed by product usage type',
                         x='day',
                         y='ctr',
                         hue='product',
                         legend_title='Product Type'
                        )

        elif plot == 'views':
            to_plot = ToPlot(data=data,
                         plot_type='lineplot',
                         ylabel='Views per Active User',
                         title='Daily Views per Active User in feed by product usage type',
                         x='day',
                         y='views per active user',
                         hue='product',
                         legend_title='Product Type'
                        )
        else:
            raise ValueError("plot parameter must be 'ctr' or 'value'")

        return to_plot

    # toplot for plot 4 - percentage of ads-sourced users by product
    @task
    def transform_ads_percentage(dfs, products):

        data = Transformer.ads_percentage_maker(dfs, products)
        to_plot = ToPlot(data=data,
                         plot_type='lineplot',
                         ylabel='% of ads-sourced active users',
                         title='% of ads-sourced active users by product usage type',
                         x='day',
                         y='ads_percentage',
                         hue='product',
                         legend_title='Product Type'
                        )

        return to_plot

    # toplot for plot 5 - feed users by retention
    @task
    def transform_retention(retention_df):

        data = retention_df.set_index('week')

        to_plot = ToPlot(data=data,
                         plot_type='barplot',
                         ylabel='Number of Users',
                         title='Feed User Groups by Week',
                         x='week',
                         hue='product',
                         legend_title='Product Type'
                        )
        return to_plot

    # toplot for plot 6 - age distribution by gender and product
    @task
    def transform_age_dist(dfs, age):
        data = Transformer.age_distribution_maker(dfs, age)
        to_plot = ToPlot(data=data,
                    plot_type='violinplot',
                    ylabel='Product type',
                    title='Users distribution by age, gender, and product used',
                    xlabel='age',
                    x='age',
                    y="product",
                    hue='gender',
                    legend_title='Gender'
                        )
        return to_plot

    # make a report plot and send it
    @task
    def report(to_plot_objects, 
               chat_id, 
               bot, 
               suptitle='daily report',
               figsize=(10, 12),
                scale=.5):

        reporter = Reporter(to_plot_objects=to_plot_objects,
                     chat_id=chat_id,
                     bot=bot,
                     suptitle=suptitle,
                   figsize=figsize,
                    scale=scale
                   )

        reporter.send_plots()
 

    ### SET THE TASK ORDER ###
    
    # get the data
    shared_df = extract_df(shared_query, connection_sim, 'shared')
    feed_df = extract_df(feed_query, connection_sim, 'feed')
    message_df = extract_df(message_query, connection_sim, 'message')
    retention_df = extract_df(retention_query, connection_sim)

    # these lists will be used for plottings as arguments
    dfs = [shared_df,
           feed_df,
           message_df
          ]

    products = ['shared',
                'feed',
                'message'
               ]

    # transform and pre-plot the data
    shared_dau_os = transform_dau_os(shared_df)
    ctr = transform_ctr(shared_df, feed_df, 'ctr')
    views = transform_ctr(shared_df, feed_df, 'views')
    ads_percentage = transform_ads_percentage(dfs, products)
    retention = transform_retention(retention_df)
    age_dist = transform_age_dist(dfs=dfs, age=50)

    # send a report plot
    report(to_plot_objects=[shared_dau_os,
                           ctr,
                           views,
                           ads_percentage,
                           retention,
                           age_dist
                          ], 
               chat_id=chat_id, 
               bot=bot, 
               suptitle='Product report - past 30 days / 8 weeks',
               figsize=(10, 12),
                scale=.5)
    
### LAUCH THE DAG ###
dag_sim_shiriaev_task_9_2 = dag_sim_shiriaev_task_9_2()
