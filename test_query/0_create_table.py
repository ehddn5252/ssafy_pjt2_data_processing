from DB.DML import DML
from DB.DDL import DDL

if __name__ == "__main__":
    dml_instance = DML()
    ddl_instance = DDL()
    ddl_instance.CREATE_TABLE('TEST')
