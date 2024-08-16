from notion_client import Client
import src.config.credentials as cred


class NotionDB_Handler:

    def __init__(self):
        self.client = Client(auth=cred.NOTION_API_KEY)

    def auto_detect_db_row_writer(self, json):

        if "ID" in json and "total" in json and "date" in json and "receipt_id" in json and "receipt_file_name" in json:
            self.client.pages.create(
                **{
                    "parent": {
                        "database_id": cred.NOTION_TRASACTION_DB_ID
                    },
                    'properties': {
                        'Trans-ID': {'title': [{'text': {'content': str(json['ID'])}}]},
                        'Total': {'number': json['total']},
                        'Date': {'date': {'start': json['date']}},
                        'Receipt ID': {'rich_text': [{'text': {'content': json['receipt_id']}}],},
                        'JPG File Name': {'rich_text': [{'text': {'content': json['receipt_file_name']}}],}

                    }
                }
            )

        elif "name" in json and "address" in json and "city" in json and "VAT" in json:
            self.client.pages.create(
                **{
                    "parent": {
                        "database_id": cred.NOTION_STORE_DB_ID
                    },
                    'properties': {
                        'Trans-ID': {'title': [{'text': {'content': str(json['ID'])}}]},
                        'Name': {'rich_text': [{'text': {'content': json['name']}}],},
                        'Address': {'rich_text': [{'text': {'content': json['address']}}],},
                        'City': {'rich_text': [{'text': {'content': json['city']}}],},
                        'VAT': {'rich_text': [{'text': {'content': json['VAT']}}],},

                    }
                }
            )
        
        elif "amount" in json[0] and "tax" in json[0] and "description" in json[0]:
            for row in json:
                self.client.pages.create(
                    **{
                        "parent": {
                            "database_id": cred.NOTION_GOOD_DB_ID
                        },
                        'properties': {
                            'Trans-ID': {'title': [{'text': {'content': str(row['ID'])}}]},
                            'Cost': {'number': row['amount']},
                            'Tax': {'number': row['tax']},
                            'Description': {'rich_text': [{'text': {'content': row['description']}}],}

                        }
                    }
                )
    

    def get_last_transaction_id(self):

        last_row = self.client.databases.query(
            database_id=cred.NOTION_TRASACTION_DB_ID,
            sorted={
                'property': 'Trans-ID',
                'direction': 'descending'
            },
            page_size=1
        )

        for data in last_row['results']:
            dot_chained_keys = 'properties.Trans-ID.title.0.plain_text'
            keys = dot_chained_keys.split('.')
            for key in keys:
                try:
                    if isinstance(data, list):
                        data = data[int(key)]
                    else:
                        data = data[key]
                except (KeyError, TypeError, IndexError):
                    return 0
            if data == None:
                return 0
            else:
                return int(data)
    

    def check_trasaction_id(self, id):
            
        result = self.client.databases.query(
            database_id=cred.NOTION_TRASACTION_DB_ID,
            filter={
                "property": "Trans-ID",  # Sostituisci "ID" con il nome della proprietà ID nel tuo database
                "rich_text": {
                    "equals": str(id)
                }
            }
        )
        if result["results"] == []:
            return False
        else: 
            return True



