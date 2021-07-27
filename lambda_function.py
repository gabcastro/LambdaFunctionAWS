import json
from settings import SCRIPT_PATH 
from settings import DB_CONN_REDSHIFT
from settings import DB_CONN_MYSQL
from utils_redshift import RedshiftDataManager
from utils_mysql import MySqlDataManager
from utils import ScriptReader

# puting outside of the handler function because for each container that lambda connect, is going to be reused 
# if I put in it be established every time that my lambda function run
mdm = MySqlDataManager(DB_CONN_MYSQL)
rdm = RedshiftDataManager(DB_CONN_REDSHIFT)

def lambda_handler(event, context):
    
    match event['environment']:
        case "production":
            redshift_script = ScriptReader.get_script(SCRIPT_PATH).format('public', event['cutoffdate'])
            mysql_script = ScriptReader.get_script(SCRIPT_PATH).format('production', event['cutoffdate'])
        case _:
            pass
            
    status_response, msg_response_r = rdm.execute_update(redshift_script)
    if status_response:
        status_response, msg_response_m = mdm.execute_update(mysql_script)
    
    return {
         'statusCode': 200 if status_response else 500,
         'body': json.dumps(msg_response_r + "/" + msg_response_m)
    }