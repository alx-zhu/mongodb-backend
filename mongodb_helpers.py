from pymongo import MongoClient
import pandas as pd
import os
from google_drive_helpers import *
from constants import *


def initial_test():
    # Connect to MongoDB
    client = MongoClient(connection_string)

    try:
        client.admin.command("ping")
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    database = client[database_name]
    collection = database[collection_name]

    print(f"Collection count: {collection.count_documents({})}")

    # Define the query
    # query = { "type": "clinical researcher" }

    # Execute the query and print the results
    # cursor = collection.find(query)
    cursor = collection.find({})

    for document in cursor:
        print(document)

    # Close the connection
    client.close()


def connect_to_mongo():
  # Connect to MongoDB
  client = MongoClient(connection_string)

  try:
      client.admin.command("ping")
      print("Pinged your deployment. You successfully connected to MongoDB!")

      database = client[database_name]
      collection = database[collection_name]
      
      return (client, database, collection)

  except Exception as e:
      print(e)
      return None
  

def populate_database(googleFolderName):
    # Get path to local destination folder, create if doesn't exist
    localFolderPath = os.path.join(os.getcwd(), "to_upload")
    if (not os.path.exists(localFolderPath)):
      os.mkdir(localFolderPath)

    # Clear the folder before downloading to avoid duplicates
    clearFolder(localFolderPath)

    # Connect to google drive
    creds = google_get_creds()

    # Find folder and download to local folder
    folderId = google_fetch_folder(creds, googleFolderName)
    filesInfo = google_download_from_folder(creds, folderId, localFolderPath)
    if (len(filesInfo) == 0):
        print("No files in the google drive folder.")

    # Connect to MongoDB
    client, database, collection = connect_to_mongo()

    # Upload all files to MongoDB
    for fileName in os.listdir(localFolderPath):
        filePath = fixFileName(os.path.join(localFolderPath, fileName))
        print(f"Inserting entries from {fileName}...", end="")
        # only process files
        if (os.path.isfile(filePath)):
          if (fileName.endswith(".csv")):
            # Read CSV file into a DataFrame
            df = pd.read_csv(filePath)

            # Convert DataFrame to dictionary
            data = df.to_dict(orient="records")

            # Insert data into MongoDB collection
            collection.insert_many(data)
            print("Successful!")

    # Close MongoDB connection
    client.close()


def fixFileName(filePath):
  newName = os.path.basename(filePath).replace(" ", "_")
  newParent = os.path.dirname(filePath)
  newPath = os.path.join(newParent, newName)
  os.rename(filePath, newPath)
  return newPath

################################ clearFolder ###################################

def clearFolder(folderPath):
  for fileName in os.listdir(folderPath):
    filePath = os.path.join(folderPath, fileName)
    if (os.path.isfile(filePath)):
      os.remove(filePath)

# populate_database("to_email")
# populate_database("emailed")
initial_test()