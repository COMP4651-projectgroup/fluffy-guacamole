from pyspark.sql import SparkSession

#after downloading yelp dataset, paste the path to the business.json file below
business_file_path = "/Users/ryantan/Downloads/Yelp JSON/yelp_dataset/yelp_academic_dataset_business.json"
if business_file_path == "":
    raise ValueError("Please set the business_file_path variable to the path of the business.json file.")

#cache for dataframe
dataframe = None
spark = None

def process_business_file(file_path):
    global dataframe
    global spark
    spark = SparkSession.builder.appName("Yelp").getOrCreate()
    dataframe = spark.read.json(file_path)

    dataframe.cache() 

    return dataframe

def get_business_details(business_id):
    """
    returns location, business type, rating, and open/closed information for a given business_id.
    """
    global dataframe
    if dataframe is None:
        raise ValueError("Business DataFrame is not loaded. Call process_business_file() first.")
    
    business_info = dataframe.filter(dataframe.business_id == business_id).select(
        "name",
        "address",
        "city",
        "state",
        "postal_code",
        "latitude",
        "longitude",
        "categories",
        "stars",    
        "is_open"    
    ).collect()

    if business_info:
        return business_info[0].asDict()
    else:
        return None

# if __name__ == "__main__":          
#     process_business_file(business_file_path)
#     #print(get_business_details("Pns2l4eNsfO8kk83dixA6A"))

#Take note
#Set up python virtual environment 
# python3 -m venv venv
# source venv/bin/activate
# pip3 install pyspark or pip install pyspark
# make sure interpreter points to venv 1. cmd+shift+p 2. select interpreter 3. select venv
#Run the script read_business.py


#Possible improvements
#Cache the dataframe response -> so that main running function can store the dataframe in cache for faster access