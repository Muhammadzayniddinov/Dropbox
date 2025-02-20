import mysql.connector
import os

def convertToBinaryData(filename):
    try:
        with open(filename, 'rb') as file:
            binaryData = file.read()
        return binaryData
    except FileNotFoundError:
        print(f"File not found: {filename}")
        raise

def insertBLOB(oi_guid, oi_data):
    try:
        cnx = mysql.connector.connect(user='root',
                                      password='Sarvar0210',
                                      host='127.0.0.1',
                                      database='drop_box')

        cursor = cnx.cursor()
        sql_insert_blob_query = """ INSERT INTO orig_images 
             (oi_guid, oi_data) 
             VALUES (%s,%s)"""

        if not os.path.isfile(oi_data):
            print(f"Invalid file path: {oi_data}")
            return

        binary_image = convertToBinaryData(oi_data)

        insert_blob_tuple = (oi_guid, binary_image)
        cursor.execute(sql_insert_blob_query, insert_blob_tuple)
        cnx.commit()
        print(f"Image {oi_guid} inserted successfully.")

    except mysql.connector.Error as error:
        print(f"Failed inserting BLOB data into MySQL table {error}")

    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print("MySQL connection is closed")

insertBLOB('i1-1-1-1', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat1(Tom Cruise)\holiday\fly.jpeg")
insertBLOB('i1-1-1-2', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat1(Tom Cruise)\holiday\tc_blog.jpeg")
insertBLOB('i1-1-2-1', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat1(Tom Cruise)\movie_photos\tgmaveric.jpeg")
insertBLOB('i1-1-2-1-1', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat1(Tom Cruise)\movie_photos\Jack Reacher\tc_jack_reacher.jpeg")
insertBLOB('i1-1-2-1-2', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat1(Tom Cruise)\movie_photos\Jack Reacher\tom_cruise_jack_reacher.jpeg")
insertBLOB('i1-1-2-2-1-1', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat1(Tom Cruise)\movie_photos\Mission Impossible\MI2\mi2_poster.jpeg")
insertBLOB('i1-1-2-2-1-2', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat1(Tom Cruise)\movie_photos\Mission Impossible\MI2\mi2_tc.jpeg")
insertBLOB('i1-1-2-2-2-1', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat1(Tom Cruise)\movie_photos\Mission Impossible\Mission Impossible 1\mission_i_tc.jpeg")

insertBLOB('i1-2-1-1', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat2(Mike Tyson)\Boxing\tyson_punch.jpeg")
insertBLOB('i1-2-1-2', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat2(Mike Tyson)\Boxing\tyson_vs_jones.jpeg")
insertBLOB('i1-2-2-1', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat2(Mike Tyson)\Daily\mt.jpeg")
insertBLOB('i1-2-2-2', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat2(Mike Tyson)\Daily\tyson1.jpeg")
insertBLOB('i1-2-2-1-1', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat2(Mike Tyson)\Daily\Family\family_photo.jpeg")
insertBLOB('i1-2-2-1-2', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat2(Mike Tyson)\Daily\Family\fpmt.jpeg")
insertBLOB('i1-2-2-2-1', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat2(Mike Tyson)\Daily\Friends\ali_tyson.jpeg")
insertBLOB('i1-2-2-2-2', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account1\Pat2(Mike Tyson)\Daily\Friends\tyson_holifild.jpeg")

insertBLOB('i2-1-1', r"C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\Structure\Account2\Pat3(John Cena)\jc.jpeg")
