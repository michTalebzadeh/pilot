from __future__ import print_function
import cx_Oracle
import sys
import csv
import google
from google.cloud import storage
from google.cloud import bigquery
import google.auth
from conf import variables as v
from conf import configs as c
class AllInOne:

  rec = {}

  def read_oracle_table(self):
    # Check Oracle is accessible
    try:
      c.conn
    except cx_Oracle.DatabaseError as e:
      print("Error: {0} [{1}]".format(e.msg, e.code))
      sys.exit(1)
    else:
      # Check if table exists
      c.cursor2.execute (c.sqlTable)
      if c.cursor2.fetchone()[0] == 1:
        print("\nTable " + v._dbschema+"."+ v._dbtable + " exists\n")
        c.cursor2.execute(c.sql)
        # get column descriptions
        columns = [i[0] for i in c.cursor2.description]
        rows = c.cursor2.fetchall()
        # write oracle data to the csv file
        csv_file = open(v.dump_dir+v.filename, mode='w')
        writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
        # write column headers to csv file
        writer.writerow(columns)
        for row in rows:
          writer.writerow(row)   ## write rows to csv file

        print("writing to csv file " + v.dump_dir+ v.filename + " complete")
        c.cursor2.close()
        c.conn.close()
        csv_file.close()
      else:
        print("Table " + v._dbschema+"."+ v._dbtable + " does not exist, quitting!")
        c.conn.close()
        sys.exit(1)

  def drop_if_bqTable_exists():
    from google.cloud.exceptions import NotFound
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(v.dataset)
    table_ref = dataset_ref.table(v.bqTable)
    try:
      bigquery_client.get_table(table_ref)
    except NotFound:
      print('table ' + v.bqTable + ' does not exist')
      return False
    try:
      print('table ' + v.bqTable + ' exists, dropping it')
      bigquery_client.delete_table(table_ref)
      return True
    except:
      print('Error deleting table ' + v.bqTable)
      sys.exit(1)

  def bq_create_table():
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(v.dataset)
    table_ref = dataset_ref.table(v.bqTable)
    schema = [
        bigquery.SchemaField(v.col_names[0], v.col_types[0], v.col_modes[0])
      , bigquery.SchemaField(v.col_names[1], v.col_types[1], v.col_modes[1])
      , bigquery.SchemaField(v.col_names[2], v.col_types[2], v.col_modes[2])
      , bigquery.SchemaField(v.col_names[3], v.col_types[3], v.col_modes[3])
      , bigquery.SchemaField(v.col_names[4], v.col_types[4], v.col_modes[4])
      , bigquery.SchemaField(v.col_names[5], v.col_types[5], v.col_modes[5])
      , bigquery.SchemaField(v.col_names[6], v.col_types[6], v.col_modes[6])
    ]
    table = bigquery.Table(table_ref, schema=schema)
    table = bigquery_client.create_table(table)
    print('table {} created.'.format(table.table_id))

  def delete_blob_if_exists_and_upload_to_GCP():
    credentials, _ = google.auth.default()
    storage_client = storage.Client(v.projectname, credentials=credentials)
    bucket = storage_client.get_bucket(v.bucketname)
    Exists = bucket.blob(v.filename).exists()
    if(Exists):
      try:
        print('file gs://' + v.bucketname + '/' + v.filename + ' exists, deleting it before uploading again')
        ## gsutil rm -r gs://etcbucket/DUMMY.csv
        bucket.blob(v.filename).delete()
        print('The file gs://' + v.bucketname + '/' + v.filename + ' deleted')
      except Exception as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)
    else:
        print('The file gs://' + v.bucketname + '/' + v.filename + ' does not exist')

    # upload blob again
    print('uploading file ' + v.filename  + ' to gs://' + v.bucketname + '/' +v.filename)
    blob = bucket.blob(v.filename)
    try:
      blob.upload_from_filename(v.dump_dir+v.filename)
      print('The file gs://' + v.bucketname + '/' + v.filename + ' was uploaded ok')
    except Exception as e:
      print("Error: {0} [{1}]".format(e.msg, e.code))
      sys.exit(1)

  def bq_load_csv_in_gcs():
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(v.dataset)
    table_ref = dataset_ref.table(v.bqTable)
    job_config = bigquery.LoadJobConfig()
    schema = [
        bigquery.SchemaField(v.col_names[0], v.col_types[0], v.col_modes[0])
      , bigquery.SchemaField(v.col_names[1], v.col_types[1], v.col_modes[1])
      , bigquery.SchemaField(v.col_names[2], v.col_types[2], v.col_modes[2])
      , bigquery.SchemaField(v.col_names[3], v.col_types[3], v.col_modes[3])
      , bigquery.SchemaField(v.col_names[4], v.col_types[4], v.col_modes[4])
      , bigquery.SchemaField(v.col_names[5], v.col_types[5], v.col_modes[5])
      , bigquery.SchemaField(v.col_names[6], v.col_types[6], v.col_modes[6])
    ]
    job_config.schema = schema
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = 'gs://'+v.bucketname+'/'+v.filename
    load_job = bigquery_client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config
    )
    try:
      print("Starting job {}".format(load_job.job_id))
      load_job.result()  # Waits for table load to complete.
      print("Job finished.")
      destination_table = bigquery_client.get_table(table_ref)
      print("Loaded {} rows.".format(destination_table.num_rows))
    except:
      print('Error loading table ' + v.bqTable)
      sys.exit(1)

  def bq_read_from_table(self):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(v.dataset)
    table_ref = dataset_ref.table(v.bqTable)
    table = bigquery_client.get_table(table_ref)
    # Specify selected fields to limit the results to certain columns
    fields = table.schema[:v.bqFields]  # first two columns
    rows = bigquery_client.list_rows(table, selected_fields=fields, max_results=v.bqRows)

    # Print row data in tabular format.
    format_string = "{!s:<16} " * len(rows.schema)
    field_names = [field.name for field in rows.schema]
    try:
      print(format_string.format(*field_names))
      for row in rows:
        print(format_string.format(*row))
    except:
      print('Error querying ' + v.dataset+"."+v.bqTable)
      sys.exit(1)
