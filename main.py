import sys
import src.sync_db_util as sync_db_util


def main():

    tran_per_time = sys.argv[1]
    
    sync_db_util.sync(int(tran_per_time))
    
if __name__ == '__main__':
    main()
