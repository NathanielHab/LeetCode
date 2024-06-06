import pandas as pd
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET
from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task

"""
This file defines an airflow DAG that tracks historical tesla stock data.
This DAG does the following:
    extracts the raw data,
    transforms it to gain useful insights,
    loads the transformed data to a SQL database,
    encodes the transformed data as an xml file
This DAG is set to run every day at 5:00.
"""


# Define the DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'Tesla_Stock_Tracker',
    default_args=default_args,
    description='An ETL pipeline using @task decorator',
    schedule_interval='0 5 * * *',  # Run daily at 5:00
    start_date=datetime(2024, 6, 1),
    tags=['TSLA'],
) as dag:

    @task
    def extract():
        # Read raw data
        df = pd.read_csv('tesla-stock-price.csv')
        return df

    @task
    def transform(df: pd.DataFrame):
        # Rename/remove columns
        df = df.rename(columns={
            'date': 'Date',
            'close': 'Close',
            'open': 'Open',
            'high': 'High',
            'low': 'Low'
        })
        df = df.drop(columns=['volume'])

        # Remove unclean data
        df = df[1:]  # Drop first row

        # Reformat dates
        df['Date'] = pd.to_datetime(df['Date'])
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

        # Add column describing percent change
        df['Percent Change'] = 100 * (df['Close'] - df['Open']) / df['Open']

        # We only want 2 decimals
        df = df.round(2)
        return df

    @task
    def load(df: pd.DataFrame):
        # Send to SQL db
        engine = create_engine('sqlite:///Test.db')
        df.to_sql('Tesla_table', con=engine, if_exists='replace', index=False)
        return df

    @task
    def encode_xml(df: pd.DataFrame):
        # Convert to XML
        root = ET.Element('root')
        for _, row in df.iterrows():
            row_elem = ET.Element('row')
            for field in row.index:
                field_elem = ET.Element(field)
                field_elem.text = str(row[field])
                row_elem.append(field_elem)
            root.append(row_elem)
        xml_data = ET.tostring(root, encoding='unicode')
        
        # Save XML file
        with open('data.xml', 'w') as f:
            f.write(xml_data)

    # Task dependencies
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load_task = load(transformed_data)
    encode_xml_task = encode_xml(load_task)

    extracted_data >> transformed_data >> load_task >> encode_xml_task