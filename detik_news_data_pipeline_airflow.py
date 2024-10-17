#dag - directed acyclic graph

#tasks : 1) fetch amazon data (extract) 2) clean data (transform) 3) create and store data in table on postgres (load)
#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres
#dependencies

from datetime import datetime, timedelta
from airflow import DAG
import requests as req
import pandas as pd
from bs4 import BeautifulSoup as bs
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from google.cloud import bigquery

header = {
    "Referer": 'https://www.detik.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}

def get_economy_data(news_, ti):
    # Base URL of the Amazon search results for economy data
    base_url = f"https://www.detik.com/search/searchnews?query=ekonomi"

    newss = []
    seen_titles = set()  # Melakukan tracking judul berita apakah sama / duplikat

    page = 1

    while len(newss) < news_:
        url = f"{base_url}&page={page}"

        # Mengirim request ke url
        response = req.get(url, headers=header)

        # Check request yang diberikan apakah success
        if response.status_code == 200:
            # Parsing content dari response url menggunakan BeautifulSoup
            soup = bs(response.content, "html.parser")

            # Mencari container semua article berita berdasarkan html
            news_container = soup.find("div", {"class": "list-content"})
            news_containers = news_container.find_all('article')
            # Loop through the book containers and extract data
            for news in news_containers:
                title = news.find("h3", {"class": "media__title"}).text.strip()
                link = news.find('a')['href']
                #description = news.find("div", {"class": "media__desc"}).text.strip()
                elements = news.select('div.media__desc')
                for element in elements:
                    print(element.text.strip())
                    description = element.text.strip()
                time = news.find("div", {"class": "media__date"})
                times = time.find("span", title=True).text.strip()
                #time = news.find("div", {"class": "media__date"}).find("span", {"class": "title"}).text.strip()
                #date = x.find('a').find('span',class_='date').text.replace('WIB','').replace('detikNews','').split(',')[1]
                #print(title)
                #print(link)
                #print(description)
                #print(times)
                if title and link and description and time:
                    news_title = title
                    # Check if title has been seen before
                    if news_title not in seen_titles:
                        seen_titles.add(news_title)
                        newss.append({ # Changed from appending a list to appending a dictionary
                            "Headline":news_title,
                            "Link":link,
                            "Description":description,
                            "Times":times
                        })
                #if title and link and description and time:
                #    news_title = title.text.strip()
                #
                #    # Check if title has been seen before
                #    if news_title not in seen_titles:
                #        seen_titles.add(news_title)
                #        news.append({
                #            "Title": news_title,
                #            "Link": link.text.strip(),
                #            "Description": description.text.strip(),
                #            "Time": time.text.strip(),
                #        })

            # Increment the page number for the next iteration
            page += 1
        else:
            print("Failed to retrieve the page")
            break

    # Limit to the requested number of books
    newss = newss[:news_]

    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(newss)

    # Remove space and dots from column
    #df = df.applymap(lambda x: x.replace(' ', '').replace('.', '') if isinstance(x, str) else x)
    #df.str.replace('[ .]', '', regex=True)

    # Push the DataFrame to XCom
    ti.xcom_push(key='data_berita', value=df.to_dict('records'))

#Function untuk cek Xcom
def pull_xcom(**kwargs):
    # Mengambil nilai dari XCom
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids='fetch_news_data')
    print(f'Value from XCom: {xcom_value}')

#3) create and store data in table on postgres (load)
    
def insert_news_data_into_postgres(ti):
    news_data = ti.xcom_pull(key='data_berita', task_ids='fetch_news_data')
    if not news_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO news_data (Headline, Link, Description, Times)
    VALUES (%s, %s, %s, %s)
    """
    for news in news_data:
        postgres_hook.run(insert_query, parameters=(news['Headline'], news['Link'], news['Description'], news['Times']))

def transfer_data_to_bigquery(**kwargs):
    # Get PostgreSQL connection details
    postgres_conn = BaseHook.get_connection('books_connection')
    postgres_engine = create_engine(f'postgresql://{postgres_conn.login}:{postgres_conn.password}@{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.schema}')

    # Load data into a DataFrame
    df = pd.read_sql('SELECT * FROM public.news_data', postgres_engine)

    # Create a BigQuery client
    bq_client = bigquery.Client.from_service_account_json('/opt/airflow/gcp_credentials/data-engineer-394322-e829687f7fd2.json')

    # Define the BigQuery table reference
    table_id = 'data-engineer-394322.news_data.news_data'

    # Write DataFrame to BigQuery
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    job.result()  # Wait for the job to complete

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
#operators : Python Operator and PostgresOperator
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_detik_news',
    default_args=default_args,
    description='A simple DAG to fetch news data from detik.com and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

#hooks - allows connection to postgres


fetch_news_data_task = PythonOperator(
    task_id='fetch_news_data',
    python_callable=get_economy_data,
    op_args=[50],  # Number of books to fetch
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS news_data (
        id SERIAL PRIMARY KEY,
        Headline TEXT NOT NULL,
        Link TEXT,
        Description TEXT,
        Times TEXT
    );
    """,
    dag=dag,
)

pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_xcom,
        provide_context=True,
    )

truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id='books_connection',  # Replace with your connection ID
        sql='TRUNCATE TABLE news_data;',  # Replace with your table name
    )

insert_news_data_task = PythonOperator(
    task_id='insert_news_data',
    python_callable=insert_news_data_into_postgres,
    dag=dag,
)

transfer_task = PythonOperator(
        task_id='transfer_postgres_to_bigquery',
        python_callable=transfer_data_to_bigquery,
        provide_context=True,
    )
#dependencies

fetch_news_data_task >> create_table_task >> pull_task >> truncate_table >> insert_news_data_task >> transfer_task