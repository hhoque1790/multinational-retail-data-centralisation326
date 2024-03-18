from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import yaml
from sqlalchemy import create_engine, inspect, text


# users = DataCleaning.clean_user_data()
# # Above clean_user_data method of DataCleaning class makes use of read_rds_table method of DataExtractor 
# # class to extract user table from database. DataExtractor class use database connector class to access
# # database.

# DatabaseConnector.upload_to_db(users)
# # upload_to_db method uploads cleaned user table to a database I have created.


# card_detailsdf = DataCleaning.clean_card_data()
# DatabaseConnector.upload_to_db(card_detailsdf,tblname='dim_card_details')

# store_data_df = DataCleaning.called_clean_store_data()
# DatabaseConnector.upload_to_db(store_data_df,tblname='dim_store_details')

# productsdf=DataCleaning.clean_products_data()
# DatabaseConnector.upload_to_db(productsdf,tblname='dim_products')


# ordersdf=DataCleaning.clean_orders_data()
# ordersdf.rename(columns={"level_0": "level__0"}, inplace=True)
# # Given column name level_0 has been changed slightly to another column name. The column name level_0 causes
# # a duplicate column error. I believe this may be because level_0 is one of the index names used for multi-level
# # indexing used across pandas Dataframes. I think somehow this causes a Duplicate column error to arise when trying
# # to upload a dataframe containing a column labellled level_0 (using the to_sql function in my upload method).
# DatabaseConnector.upload_to_db(ordersdf,tblname='orders_table')


# eventsdata=DataCleaning.clean_dim_date_times()
# DatabaseConnector.upload_to_db(eventsdata,tblname='dim_date_times')


