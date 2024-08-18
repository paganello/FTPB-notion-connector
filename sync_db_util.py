from src.SQL_Handler import SQL_Handler
from src.NotionDB_Handler import NotionDB_Handler


def sync_all():

    Notion_client = NotionDB_Handler()
    SQL_client = SQL_Handler()

    SQL_client.connect()
    trasaction_number = SQL_client.count_trasactions()
    #print("Last SQL Transaction ID: ", last_SQL_transaction_id)

    ids = SQL_client.get_all_ids()
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

if __name__ == '__main__':
    sync_all()
