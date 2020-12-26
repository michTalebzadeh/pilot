import cx_Oracle
import csv
import conf.variables as v

def test_oracle_dsn_tns():
    dsn_tns = cx_Oracle.makedsn(v.oracleHost, v.oraclePort, service_name=v.serviceName)
    conn = cx_Oracle.connect(v._username, v._password, dsn_tns)
    assert conn
    cursor = conn.cursor()
    sqlTable = "SELECT COUNT(1) from USER_TABLES WHERE TABLE_NAME = '" + v._dbtable + "'"
    sql = "SELECT ID, CLUSTERED, SCATTERED, RANDOMISED, RANDOM_STRING, SMALL_VC, PADDING FROM " + v._dbschema + "." + v._dbtable + " WHERE ROWNUM <= 10"
    assert cursor.execute(sqlTable)
    assert cursor.fetchone()[0] == 1
    columns = [i[0] for i in cursor.description]
    assert columns
    rows = cursor.fetchall()
    csv_file = open(v.dump_dir + v.filename, mode='w')
    assert csv_file
    writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
    assert writer
    assert writer.writerow(columns)
    for row in rows:
      assert writer.writerow(row)