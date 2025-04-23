import pandas as pd
from db_conn import sqlalchemy_conn
import sys
import argparse

def check_replica(standby,envfile):
    try:
        check_rep = """
                        show replica status;
                        """
        dbconn = sqlalchemy_conn(standby,envfile)
        execute = pd.read_sql(check_rep,dbconn)
        data = execute[["Source_Host","Source_Log_File","Read_Source_Log_Pos","Replica_IO_Running","Replica_SQL_Running","Replica_SQL_Running_State","Executed_Gtid_Set"]]
        print(data)
        return data['Executed_Gtid_Set'].iloc[0]

    except Exception as error:
        print(error)
        sys.exit()
    
def check_master(primary,envfile):
    try:
        check_mstr = """
                    show master status;
                    """
        dbconn = sqlalchemy_conn(primary,envfile)
        execute = pd.read_sql(check_mstr,con=dbconn)
        print(execute)
        return execute['Executed_Gtid_Set'].iloc[0]
    except Exception as error:
        print(error)


def check_replication(primrole,stbrole,envfile):
    print("\n====== check master and replica =======\n")
    print("=== master GTID ===")
    gtid_master = check_master(primrole,envfile)
    print("\n")
    print("=== replica GTID ===")
    gtid_replica = check_replica(stbrole,envfile)



    if gtid_master == gtid_replica:
        print("\n\n\n=== MySQL Replication Setup is Successful ===")
    else:
        print(f"\n\n\n=== gtid between master and replica is not the same \n gtid master : {gtid_master} gtid replica : {gtid_replica}\n please check again")
    print("\n====== completed =======\n")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="mysql replication",description="mysql replication")
    parser.add_argument('-e','--env-file',required=True,help="env file that will be use as base configuration")
    parser.add_argument('-p','--primary-role',required=True,help="database primary role host for replication and backup")
    parser.add_argument('-s','--standby-role',required=True,help="database secondary role host for replication")
    args = vars(parser.parse_args())
    check_replication(args['primary_role'],args['standby_role'],args['env_file'])