import cx_Oracle
import pprint
import csv
import sys
import conf.variables as v
import conf.configs as c

class runOracle:

  def run_oracle_module():
    dsn_tns = cx_Oracle.makedsn(v.oracleHost, v.oraclePort, service_name=v.serviceName)
    conn = cx_Oracle.connect(v._username, v._password, dsn_tns)
    cursor = conn.cursor()
    sqlTable = "SELECT COUNT(1) from USER_TABLES WHERE TABLE_NAME = '"  +v._dbtable + "'"
    sql="SELECT ID, CLUSTERED, SCATTERED, RANDOMISED, RANDOM_STRING, SMALL_VC, PADDING FROM " + v._dbschema + "." + v._dbtable + " WHERE ROWNUM <= 10"
    # Check Oracle is accessible
    try:
      conn
    except cx_Oracle.DatabaseError as e:
      print("Error: {0} [{1}]".format(e.msg, e.code))
      sys.exit(1)
    else:
      # Check if table exists
      print(sqlTable)
      cursor.execute (sqlTable)
      if cursor.fetchone()[0] == 1:
        print("\nTable " + v._dbschema+"."+ v._dbtable + " exists\n")
        cursor.execute(sql)
        # get column descriptions
        columns = [i[0] for i in cursor.description]
        rows = cursor.fetchall()
        # write oracle data to the csv file
        csv_file = open(v.dump_dir+v.filename, mode='w')
        writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
        # write column headers to csv file
        writer.writerow(columns)
        for row in rows:
          writer.writerow(row)   ## write rows to csv file
        print("writing to csv file " + v.dump_dir+ v.filename + " complete")
        cursor.close()
        conn.close()
        csv_file.close()
      else:
        print("Table " + v._dbschema+"."+ v._dbtable + " does not exist, quitting!")
        conn.close()
        sys.exit(1)
