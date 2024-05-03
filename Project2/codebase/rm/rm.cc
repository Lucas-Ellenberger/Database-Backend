
#include "rm.h"

#define table "Tables"
#define column "Columns"

RelationManager* RelationManager::_rm = 0;
RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
    RecordBasedFileManager* catalog = NULL;
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    // we store each table here by name and id,
    // we will storer how that table is stored (ig just paged memory?)
    // for each attribute, we will store name and type
    // we do not have fixed length so we cannot store length. 
    // we do not have any indexes so we will not store any information about indexes, same goes for constraints. 

    catalog = RecordBasedFileManager::instance(); 

    RC rc;
    
    rc = catalog->createFile("Tables");
    if (rc != SUCCESS) {
        cerr << "Unable to create Tables file" << endl;
    }
    rc = catalog->openFile("Tables", this.tableHandle);
    if (rc != SUCCESS) {
        cerr << "Unable to open Tables file" << endl;
    }


    //create "tables" table
    vector<Attribute> tableDescriptor;
    createTableRecordDescriptor(tableDescriptor);
    //despite no fields ever being null, we will create the nulls indicator
    int tableNullFieldsIndicatorActualSize = ceil((double) tableDescriptor.size() / CHAR_BIT);
    unsigned char *nullsIndicatorTable = (unsigned char *) malloc(tableNullFieldsIndicatorActualSize);
    memset(nullsIndicatorTable, 0, tableNullFieldsIndicatorActualSize);

    //now create records
    RID rid;
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);

    prepareTableRecord(tableDescriptor.size(), nullsIndicatorTable, 1, "Tables", "Tables", record, &recordSize); //not sure we actually need recordSize
    rc = catalog->insertRecord(tableHandle, tableDescriptor, record, rid);
    if (rc != SUCCESS) {
        cerr << "Unable to insert \"Table\" record into table Table" << endl;
    }
    prepareTableRecord(tableDescriptor.size(), nullsIndicatorTable, 2, "Columns", "Columns", record, &recordSize);
    rc = catalog->insertRecord(tableHandle, tableDescriptor, record, rid);
    if (rc != SUCCESS) {
        cerr << "Unable to insert \"Column\" record into table Table" << endl;
    }



    FileHandle columnHandle;
    rc = catalog->createFile("Columns");
    if (rc != SUCCESS) {
        cerr << "Unable to create Columns file" << endl;
    }
    rc = catalog->openFile("Columns", this.columnHandle);
    if (rc != SUCCESS) {
        cerr << "Unable to open Columns file" << endl;
    }

    //create "columns" table
    vector<Attribute> columnDescriptor;
    createColumnRecordDescriptor(columnDescriptor);
    //despite no fields ever being null, we will create the nulls indicator
    int columnNullFieldsIndicatorActualSize = ceil((double) columnDescriptor.size() / CHAR_BIT);
    unsigned char *nullsIndicatorColumn = (unsigned char *) malloc(columnNullFieldsIndicatorActualSize);
    memset(nullsIndicatorColumn, 0, columnNullFieldsIndicatorActualSize);

    //now create records
    recordSize = 0;


    string table_names[3] = {"table-id", "table-name", "file-name"};
    AttrType table_types[3] = {TypeInt, TypeVarChar, TypeVarChar};
    int table_size[3] = {4, 50, 50};
    for(int i = 0; i < 3; i += 1) {
        prepareColumnRecord(columnDescriptor.size(), nullsIndicatorColumn, 1, table_names[i], table_types[i], table_size[i], i+1, record, &recordSize); //not sure we actually need recordSize
        rc = catalog->insertRecord(columnHandle, columnDescriptor, record, rid);
        if (rc != SUCCESS) {
            cerr << "Unable to insert \"Table\" record number " << i << " into table Column" << endl;
        }
    }

    string column_names[5] = {"table-id", "column-name", "column-type", "column-length", "column-position"};
    AttrType column_types[5] = {TypeInt, TypeVarChar, TypeInt, TypeInt, TypeInt};
    int column_size[5] = {4, 50, 4, 4, 4};
    for(int i = 0; i < 5; i += 1) {
        prepareColumnRecord(columnDescriptor.size(), nullsIndicatorColumn, 2, column_names[i], column_types[i], column_size[i], i+1, record, &recordSize); //not sure we actually need recordSize
        rc = catalog->insertRecord(columnHandle, columnDescriptor, record, rid);
        if (rc != SUCCESS) {
            cerr << "Unable to insert \"Table\" record number " << i << " into table Column" << endl;
        }
    }
    table_id_count = 3;
    free(nullsIndicatorTable);
    free(nullsIndicatorColumn);
    free(record);
    free(returnedData);

    // After opening these files, I do not close them. I do not know if this the expected behavior or if I should close them
    return SUCCESS;
}

RC RelationManager::deleteCatalog()
{
    if (catalog == NULL) return CATALOG_DSN_EXIST;

    catalog->closeFile(this.tableHandle);
    catalog->closeFile(this.columnHandle);
    catalog->destroyFile("Tables");
    catalog->destroyFile("Columns");

    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    // have to check if table exists already, only way I see is by going through the catalog to see if there is already a table of the same name (idk how to do that yet)
    RC rc;
    rc = catalog->createFile(tableName);
    if (rc != SUCCESS) {
        cerr << "Unable to create " << tableName << " file" << endl;
    }
    FileHandle handle;
    rc = catalog->openFile(tableName, handle);
    if (rc != SUCCESS) {
        cerr << "Unable to open " << tableName << " file" << endl;
    }

    // prep variables for record
    RID rid;
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);

    // prep variables for new table entry
    vector<Attribute> tableDescriptor;
    createTableRecordDescriptor(tableDescriptor);
    //despite no fields ever being null, we will create the nulls indicator
    int tableNullFieldsIndicatorActualSize = ceil((double) tableDescriptor.size() / CHAR_BIT);
    unsigned char *nullsIndicatorTable = (unsigned char *) malloc(tableNullFieldsIndicatorActualSize);
    memset(nullsIndicatorTable, 0, tableNullFieldsIndicatorActualSize);

    prepareTableRecord(tableDescriptor.size(), nullsIndicatorTable, table_id_count, tableName, tableName, record, &recordSize);
    rc = catalog->insertRecord(handle, tableDescriptor, record, rid);
    if (rc != SUCCESS) {
        cerr << "Unable to insert " << tableName << " record into table Table" << endl;
    }

    int num_attrs = attrs.size();
    for (int i = 0; i < num_attrs; i += 1) {
        prepareColumnRecord(columnDescriptor.size(), nullsIndicatorColumn, 2, attrs[i].name, attrs[i].type, attrs[i].length, i+1, record, &recordSize); //not sure we actually need recordSize
        rc = catalog->insertRecord(columnHandle, columnDescriptor, record, rid);
        if (rc != SUCCESS) {
            cerr << "Unable to insert " << tableName << " record number " << i << " into table Column" << endl;
        }
    }

    table_id_count += 1;
    catalog->closeFile(handle);
    free(record);
    free(returnedData);
}

RC RelationManager::deleteTable(const string &tableName)
{
    // go into Tables table, find table with name tableName, get its table-id, delete that record
    // go into Columns table, find all records with matching table-id, and delete all of them. 

    catalog->destroyFile(tableName);
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    // Checks if table exists in catalog
    /*if (tableName){
        return TB_DN_EXIST;
    }
    */
   


    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}

// nameLength: size of record descriptor
void prepareTableRecord(const int nameLength, const string &name, const int table_id, const string &table_name, const string &file_name, void *buffer, int *recordSize)
{
    int offset = 0;

    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;

    memcpy((char *)buffer + offset, &table_id, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, table_name, table_name.size() + 1); // plus 1 for null terminator
    offset += table_name.size() + 1;

    memcpy((char *)buffer + offset, file_name, file_name.size()+1); //plus 1 for null terminator
    offset += file_name.size()+1;

    *recordSize = offset;
}

void prepareColumnRecord(const int nameLength, const string &name, const int table_id, const string column_name, const int column_type, 
                         const int column_length, const int column_position, void *buffer, int *recordSize)
{
    int offset = 0;

    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;

    memcpy((char *)buffer + offset, &table_id, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, column_name, column_name.size() + 1);
    offset += column_name.size() + 1;

    memcpy((char *)buffer + offset, &column_type, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &column_length, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &column_position, sizeof(int));
    offset += sizeof(int);

    *recordSize = offset;
}

void createTableRecordDescriptor(vector<Attribute> &recordDescriptor) {

    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);
}

void createColumnRecordDescriptor(vector<Attribute> &recordDescriptor) {

    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);
}
