import pandas as pd
from pathlib import Path
import argparse
import inquirer
import sys
import subprocess
from dotenv import load_dotenv
import os
from datetime import datetime,date
import time
from db_conn import sqlalchemy_conn

def get_config(env,dbrole):
    env_path = Path(env)
    load_dotenv(env_path)
    today_date = datetime.today().strftime('%d%m%Y')
    host = list(os.environ['HOST'].split(","))
    backup_dir = list(os.environ['BACKUP_DIR'].split(","))
    user = list(os.environ['UNAME'].split(","))
    password = list(os.environ['PASSWORD'].split(","))
    role = list(os.environ['DB_ROLE'].split(","))
    df = pd.DataFrame(list(zip(host,backup_dir,user,password,role)),columns=['host','backup_dir','user','password','role'])
    data = df.query("host == @dbrole")
    iphost = data['host'].iloc[0]
    bdir = data['backup_dir'].iloc[0]
    username = data['user'].iloc[0]
    passwd = data['password'].iloc[0]
    return iphost,bdir,username,today_date,passwd

def check_backup_directory(dirpath):
    backup_dir = Path(f'{dirpath}')
    try:
        os.makedirs(backup_dir)
    except Exception as error:
        print("create directory failed: ",error)
        sys.exit()

def get_last_position(env,dbrole):
     iphost,bdir,username,today_date,passwd = get_config(env,dbrole)
     backup_dir = f'{bdir}{today_date}'
     checkpoint_file = Path(f"{backup_dir}/xtrabackup_binlog_info")
     with open(checkpoint_file,"r") as file:
          content = file.readlines()
          for line in content:
               if line.startswith("binlog"):
                    raw_line = " ".join(line.split())
                    line_list = raw_line.replace(" ",",").split(",")
                    break
          filename = line_list[0]
          position = line_list[1]
          return filename,position

def backup_database(env,dbrole):
    try:
        get_config(env,dbrole)
        iphost,bdir,username,today_date,passwd = get_config(env,dbrole)
        backup_dir = f'{bdir}{today_date}'
        check_backup_directory(backup_dir)
        backup_cmd1 = subprocess.Popen(["ssh",f"{iphost}","xtrabackup","--compress","--backup","-u",f"{username}",f"-p{passwd}",f"--host={iphost}","--stream=xbstream",f"--target-dir={backup_dir}"],stdout=subprocess.PIPE)
        backup_cmd2 = subprocess.Popen(["xbstream","-x","-C",f"{backup_dir}"],stdin=backup_cmd1.stdout,stdout=subprocess.PIPE)
        execute_cmd = backup_cmd2.communicate()

    except Exception as error:
        print(error)
        sys.exit()

def extract_prepare(env,dbrole):
    iphost,bdir,username,today_date,passwd = get_config(env,dbrole)
    backup_dir = f'{bdir}{today_date}'
    try:
        extract = subprocess.run(["xtrabackup","--decompress",f"--target-dir={backup_dir}"],stdout=subprocess.PIPE)
        #extract.communicate()
    except Exception as error:
        print(error)
        sys.exit()

    try:
        prepare = subprocess.run(["xtrabackup","--prepare",f"--target-dir={backup_dir}"],stdout=subprocess.PIPE)
        #prepare.communicate()
    except Exception as error:
        print(error)
        sys.exit()

def change_replica(primary,standby,envfile):
    iphost,bdir,username,today_date,passwd = get_config(envfile,standby)
    change_rep = f"""
                    CHANGE REPLICATION SOURCE TO
                    SOURCE_HOST='{primary}',
                    SOURCE_USER='{username}',
                    SOURCE_PASSWORD='{passwd}',
                    SOURCE_AUTO_POSITION = 1;
                    """
    try:
        dbconn = sqlalchemy_conn(standby,envfile)
        execute = pd.read_sql(change_rep,dbconn)

    except Exception as error:
        print(error)
        #sys.exit()

def start_replica(standby,envfile):
    try:
        start_rep = """
                        START REPLICA;                
                        """
        dbconn = sqlalchemy_conn(standby,envfile)
        execute = pd.read_sql(start_rep,dbconn)

    except Exception as error:
        print(error)
        #sys.exit()
        
def stop_replica(standby,envfile):
    try:
        stop_rep = """
                    stop replica;
                    """
        dbconn = sqlalchemy_conn(standby,envfile)
        execute = pd.read_sql(stop_rep,dbconn)

    except Exception as error:
        print(error)
        #sys.exit()


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

def restore_backup(env,dbrole,mycnf):
    iphost,bdir,username,today_date,passwd = get_config(env,dbrole)
    backup_dir = f'{bdir}{today_date}'
    try:
        restore = subprocess.run(["xtrabackup",f"--defaults-file={mycnf}","--copy-back",f"--target-dir={backup_dir}"],stdout=subprocess.PIPE)
        time.sleep(5)
        change_ownership = subprocess.run(["chown","-R","mysql:mysql","/var/lib/mysql/"],stdout=subprocess.PIPE)
        time.sleep(5)
        startup_service = subprocess.run(["systemctl","start","mysql"],stdout=subprocess.PIPE)

    except Exception as error:
        print(error)
        sys.exit()


def replication_setup(envfile,primrole,stbrole,cfile):
    print(envfile,primrole,stbrole)
    backup_question = [inquirer.List('backup',message="do you want to backup primary database now? [yes/no]",choices=['yes','no']),]
    backup_answer = inquirer.prompt(backup_question)

    if backup_answer['backup'] == 'yes':
        print("\n====== creating backup  at ",datetime.today().strftime('%d%m%Y %H:%M:%S'),"=======\n")
        backup_database(envfile,primrole)
        print("\n====== Backup Completed at ",datetime.today().strftime('%d%m%Y %H:%M:%S'),"=======\n")
    else:
        print('create replication setup failed : backup canceled')

    print("\n====== extracting backup  at ",datetime.today().strftime('%d%m%Y %H:%M:%S'),"=======\n")
    extract_prepare(envfile,stbrole)
    print("\n====== Extract backup completed at ",datetime.today().strftime('%d%m%Y %H:%M:%S'),"=======\n")

    print("\n====== restore backup files =======\n")
    restore_backup(envfile,stbrole,cfile)
    print("\n====== restore backup completed =======\n")
    
    print("\n====== Setup MySQL Replica =======\n")
    change_replica(primrole,stbrole,envfile)

    print("\n====== start replica =======\n")
    start_replica(stbrole,envfile)
    print("\n====== completed ======")

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
    parser.add_argument('-c','--config-file',required=True,help="mysql standby db my.cnf config file full path")
    args = vars(parser.parse_args())
    replication_setup(args['env_file'],args['primary_role'],args['standby_role'],args['config_file'])

