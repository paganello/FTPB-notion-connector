from src.SQL_Handler import SQL_Handler
from src.NotionDB_Handler import NotionDB_Handler
from src.config.credentials import default_tras_number_to_verify_per_time 


def sync(n = default_tras_number_to_verify_per_time):

    Notion_client = NotionDB_Handler()
    SQL_client = SQL_Handler()

    SQL_client.connect()

    tras_number = int(SQL_client.count_trasactions())
    
    if tras_number < n:
        n = tras_number

    ids = SQL_client.get_last_n_ids(n)
    #ids = SQL_client.get_all_ids()
    #print(ids)
    for i in ids:
        if Notion_client.check_trasaction_id(i["ID"]) == False:
            #print("Transaction ID: ", i["ID"])
            insertion = SQL_client.read_insertion_by_id(i["ID"])
            SQL_client.extract_insertion_data(insertion)

            Notion_client.auto_detect_db_row_writer(SQL_client.trasaction_json)
            Notion_client.auto_detect_db_row_writer(SQL_client.store_json)
            Notion_client.auto_detect_db_row_writer(SQL_client.good_json)

    SQL_client.close_connection()

