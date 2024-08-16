
from src.SQL_Handler import SQL_Handler
from src.NotionDB_Handler import NotionDB_Handler


def main():
    
    Notion_client = NotionDB_Handler()
    SQL_client = SQL_Handler()

    last_notion_transaction_id = Notion_client.get_last_transaction_id()
    #print("Last Notion Transaction ID: ", last_notion_transaction_id)

    SQL_client.connect()
    last_SQL_transaction_id = SQL_client.read_last_transaction_id()
    #print("Last SQL Transaction ID: ", last_SQL_transaction_id)

    if last_SQL_transaction_id != last_notion_transaction_id:
        insertion = SQL_client.read_insertion_by_id(last_SQL_transaction_id)
        SQL_client.extract_insertion_data(insertion)

        Notion_client.auto_detect_db_row_writer(SQL_client.trasaction_json)
        Notion_client.auto_detect_db_row_writer(SQL_client.store_json)
        Notion_client.auto_detect_db_row_writer(SQL_client.good_json)

        SQL_client.close_connection()

    else:
        SQL_client.close_connection()

if __name__ == '__main__':
    main()
