# nyc taxi ingestion
     database -> pipline ->Big data

# Configure a PostgreSQL container using Docker Compose
     Refer to the Docker Compose file

# Download the file from the link : 
    https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet

# Ensure you have Pandas and a Parquet library installed. 

    pip install pandas pyarrow

    You can refer to the notebook file load_data.ipynb 

# convert our load_data notebook to a python script

     jupyter nbconvert --to=script load_data.ipynb 