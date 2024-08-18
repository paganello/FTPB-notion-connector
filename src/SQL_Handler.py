import mysql.connector
import json
from src.config.credentials import mysql_config as config


class SQL_Handler:

    def __init__(self):
        pass

    def connect(self):
        self.conn = mysql.connector.connect(**config)
        self.cursor = self.conn.cursor(dictionary=True)


    #def read_last_transaction_id(self):
    #
    #    query = """
    #        SELECT transaction.ID
    #        FROM transaction 
    #        ORDER BY ID DESC 
    #        LIMIT 1"""
    #    
    #    self.cursor.execute(query)
    #    ultima_riga = self.cursor.fetchone()
    #
    #    if ultima_riga["ID"] is None:
    #        return 0
    #
    #    return ultima_riga["ID"]
    

    def count_trasactions(self):
        query = """
            SELECT COUNT(transaction.ID) AS count
            FROM transaction"""
        
        self.cursor.execute(query)
        data = self.cursor.fetchone()
        return data["count"]
    

    #def get_all_ids(self):
    #    query = """
    #        SELECT transaction.ID
    #        FROM transaction"""
    #    
    #    self.cursor.execute(query)
    #    data = self.cursor.fetchall()
    #    return data
    
    def get_last_n_ids(self, n):
        query = f"""
            SELECT transaction.ID
            FROM transaction 
            ORDER BY ID DESC 
            LIMIT {n}"""
        
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        return data


    def read_insertion_by_id(self, id):

        query = f"""
            SELECT transaction.ID, transaction.date, transaction.total, transaction.receipt_ID, transaction.receipt_file_name, store.name, store.address, store.city, store.VAT, good.amount, good.tax, good.description 
            FROM transaction 
            JOIN store ON transaction.ID = store.transaction_ID 
            JOIN good ON transaction.ID = good.transaction_ID 
            WHERE transaction.ID = {id}"""
        
        self.cursor.execute(query)
        data = self.cursor.fetchall()

        #print(data)
        
        return data    
    
    
    def extract_insertion_data(self, data):
        self.trasaction_json= {
            "ID": data[0]["ID"],
            "date": data[0]["date"].strftime("%Y-%m-%d %H:%M:%S"),
            "total": data[0]["total"],
            "receipt_id": data[0]["receipt_ID"],
            "receipt_file_name": data[0]["receipt_file_name"],
        }

        self.store_json = {
                "ID": data[0]["ID"],
                "name": data[0]["name"],
                "address": data[0]["address"],
                "city": data[0]["city"],
                "VAT": data[0]["VAT"],
        }

        self.good_json = []
        for row in data:
            new = {
                "ID": data[0]["ID"],
                "amount": row["amount"],
                "tax": row["tax"],
                "description": row["description"],
            }
            self.good_json.append(new)

        #print(self.trasaction_json)
        #print(self.store_json)
        #print(self.good_json)

    


    def close_connection(self):
        self.cursor.close()
        self.conn.close()
    



