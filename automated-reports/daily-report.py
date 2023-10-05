### DESCRIPTION ###

# Here, we set up an automated reporting system using Airflow.
# The system performs an ETL task and reports via corporate messenger chat 
# (Telegram in this case).
# The report includes key daily metrics for the past week:
# DAU, likes, views, CTR.

# An example of the report is shown in the file 'daily-report.jpg'

### IMPORTS ###

# hide database access passwords
import os
from dotenv import load_dotenv

# file work and time
import io
from datetime import datetime, timedelta

# suppress matplotlib warnings
import warnings
import logging

# calculation and data manipulation
import pandas as pd
import pandahouse as ph # Connect with clickhouse DB

# plotting
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import seaborn as sns

# report bot-related methods
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

### DEFINE FUNCTIONS WE WOULD NEED

# Create a function that plots a lineplot on a given grid
def lineplotter(
    data,
    x: str,
    y: str,
    ax,
    hue: str = None,
    title: str = None,
    xlabel: str = None,
    ylabel: str = None,
    legend: bool = True,
    to_format_date=False,
    ) -> None:
    """
    Plots a lineplot for the given dataframe (table) on the given axis (ax)
    """
    fontsize = 7
    labelsize = 6
    
    # Check the label values and infer those not set from the table:
    if not ylabel:
        ylabel = y
    if not title:
        title = f"{ylabel} vs {xlabel}"
        
    # Plot the line plot
    sns.lineplot(
        data=data,
        x=x, 
        y=y, 
        ax=ax,
        hue=hue,
        linewidth=1,
        legend=legend,
        color='green',
        marker="s",
        markerfacecolor='white',
        markeredgecolor='black',
    )
    
    ax.set_title(title, fontsize=fontsize)
    ax.set_xlabel(xlabel, fontsize=fontsize)
    ax.set_ylabel(ylabel, fontsize=fontsize)
    
    if legend:
        ax.legend(fontsize=fontsize)

    # Set the tick labels
    
    ax.tick_params(axis='x', rotation=45, labelsize=labelsize)
    ax.tick_params(axis='y', rotation=0, labelsize=labelsize)
    
    if to_format_date:
        # set the format of the date
        ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%b %d'))  # Month Day format

def save_send_plot(chat_id, bot):
    def decorator(func):
        def wrapper(df, date_column):
            func(df, date_column)
            plot_object = io.BytesIO()
            plt.savefig(plot_object, dpi=200)
            plot_object.seek(0)
            plot_object.name = 'test_plot.png'
            plt.close()
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        return wrapper
    return decorator 

def send_report(chat_id, bot):
    def decorator(func):
        def wrapper(df, date_column='day', day_offset=0):
            message = func(df, date_column, day_offset)
            
            bot.sendMessage(chat_id=chat_id, 
                            text=message, 
                            parse_mode='HTML'
                           ) 
        return wrapper
    return decorator 

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
      'database': 'simulator_20230720',
      'user': 'student', 
      'password': db_pwd
     }

# to account for database issue, not look at past recent days (which may be absent in our db)
# set it to 0 to get data for yesterday (text) and past 7 days (plots)
day_offset = 0

# query to get data for the report
query = f"""
SELECT
    toDate(time) AS day,
    count(DISTINCT user_id) AS AU,
    countIf(action = 'like') AS likes,
    countIf(action = 'view') AS views,
    round(likes * 100 / views, 2) AS CTR
FROM simulator_20230720.feed_actions
WHERE toDate(time) BETWEEN (today() - 7 - {day_offset}) AND today() - 1 - {day_offset}
GROUP BY toDate(time)
            """

bot = telegram.Bot(token=bot_token) # bot access

### DAG CREATION ###

# create a dag
@dag(
    default_args=default_args, 
    schedule_interval=schedule_interval, 
    catchup=False
)
def dag_sim_shiriaev_task_9_1():  
    
    # get a df to work with
    @task()
    def extract(query, connection):
        # import a table to a dataframe
        df_cube = ph.read_clickhouse(
            query=query,
            connection=connection
        )
        return df_cube.rename(columns={'CTR': 'CTR (%)'})
    
    # make plots and send them in tg
    @task()
    @save_send_plot(chat_id=chat_id, bot=bot)
    def make_report_plot(df, date_column='day'):  

        # make a canvas, set sizes
        fig, axs = plt.subplots(2, 2, figsize=(10, 7))

        # make a fig title
        start_date = df[date_column].iloc[0].date()
        end_date = df[date_column].iloc[-1].date()
        fig.suptitle(f"Daily metrics from {start_date} to {end_date}", fontsize=14)

        for metric, ax in zip(df.columns[1:], axs.reshape(-1)):

            lineplotter(
                data=df,
                x=date_column,
                y=metric,
                ax=ax,
                title=f'daily {metric}',
                ylabel=f'{metric}',
                legend=False,
                to_format_date=True,
                )

        plt.tight_layout(h_pad=5)
    
    # make text report and send it in tg
    @task()
    @send_report(chat_id=chat_id, bot=bot)
    def make_report_text(
        df, 
        date_column='day', 
        day_offset=0
    ):
        # Calculate target_day's date
        target_day = datetime.now().date() - timedelta(days=day_offset + 1)

        # Create a mask for rows with date = target_day
        mask = df[date_column].dt.date == target_day

        # Apply the mask to the DataFrame
        filtered_df = df[mask]

        # make a template for a report
        message_start = \
        "<pre>Daily metrics report:\ndate:"\
        f" {target_day.strftime('%d %b')} "\
        f"({day_offset + 1} day(s) ago).\n" + '-' * 27 + '\n'

        message_end = \
        '-' * 27 +\
        f"\nError reference:\n"\
        f"NaN -  no values present for the day.\n"\
        f'inf - multiple "day" rows correspond to the same day.\n</pre>\n' 

        report_df = pd.DataFrame(columns=['metric','value']).set_index('metric')
        # iterate over the df columns and append the values to the template
        for column in df.columns[1:]:

            if filtered_df.shape[0] == 0:
                report_df.loc[column, 'value'] = pd.NA

            elif filtered_df.shape[0] > 1:
                report_df.loc[column, 'value'] = 'inf'

            else:
                report_df.loc[column, 'value'] = filtered_df[column].iloc[0]

        table = message_start + report_df.to_csv(sep='|',).replace('|',  ' '*5) + message_end

        return table

 
    # get the data
    df_to_report = extract(query, connection_sim)
    
    # send a text report
    make_report_text(df_to_report, day_offset=day_offset)
    
    # send plots
    make_report_plot(df=df_to_report, date_column='day')


# launch the dag
dag_sim_shiriaev_task_9_1 = dag_sim_shiriaev_task_9_1()
