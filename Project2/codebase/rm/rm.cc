
#include "rm.h"
#include <cstring>
#include <iostream>
#include <cmath>

#define table "Tables"
#define column "Columns"

RelationManager *RelationManager::_rm = 0;
RelationManager *RelationManager::instance()
{
    if (!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
    RecordBasedFileManager *catalog = NULL;
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
    if (rc != SUCCESS)
    {
        cerr << "Unable to create Tables file" << endl;
        return rc;
    }
    rc = catalog->openFile("Tables", (*tableHandle));

    if (rc != SUCCESS)
    {
        cerr << "Unable to open Tables file" << endl;
        return rc;
    }

    // create "tables" table
    vector<Attribute> tableDescriptor;
    createTableRecordDescriptor(tableDescriptor);
    // despite no fields ever being null, we will create the nulls indicator
    int tableNullFieldsIndicatorActualSize = ceil((double)tableDescriptor.size() / CHAR_BIT);
    char nullsIndicatorTable[tableNullFieldsIndicatorActualSize];
    memset(nullsIndicatorTable, 0, tableNullFieldsIndicatorActualSize);
    string nullsIndicatorTableString(nullsIndicatorTable);

    // now create records
    RID rid;
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);

    prepareTableRecord(tableDescriptor.size(), nullsIndicatorTableString, 1, "Tables", "Tables", record, &recordSize); // not sure we actually need recordSize
    rc = catalog->insertRecord((*tableHandle), tableDescriptor, record, rid);

    if (rc != SUCCESS)
    {
        cerr << "Unable to insert \"Table\" record into table Table" << endl;
        return rc;
    }

    prepareTableRecord(tableDescriptor.size(), nullsIndicatorTable, 2, "Columns", "Columns", record, &recordSize);
    rc = catalog->insertRecord((*tableHandle), tableDescriptor, record, rid);
    if (rc != SUCCESS)
    {
        cerr << "Unable to insert \"Column\" record into table Table" << endl;
        return rc;
    }

    FileHandle columnHandle;
    rc = catalog->createFile("Columns");
    if (rc != SUCCESS)
    {
        cerr << "Unable to create Columns file" << endl;
        return rc;
    }

    rc = catalog->openFile("Columns", (*this->columnHandle));
    if (rc != SUCCESS)
    {
        cerr << "Unable to open Columns file" << endl;
        return rc;
    }

    // create "columns" table
    vector<Attribute> columnDescriptor;
    createColumnRecordDescriptor(columnDescriptor);
    // despite no fields ever being null, we will create the nulls indicator
    int columnNullFieldsIndicatorActualSize = ceil((double)columnDescriptor.size() / CHAR_BIT);
    char nullsIndicatorColumn[columnNullFieldsIndicatorActualSize];
    memset(nullsIndicatorColumn, 0, columnNullFieldsIndicatorActualSize);
    string nullsIndicatorColumnString(nullsIndicatorColumn);

    // now create records
    recordSize = 0;

    string table_names[3] = {"table-id", "table-name", "file-name"};
    AttrType table_types[3] = {TypeInt, TypeVarChar, TypeVarChar};
    int table_size[3] = {4, 50, 50};
    for (int i = 0; i < 3; i += 1)
    {
        prepareColumnRecord(columnDescriptor.size(), nullsIndicatorColumnString, 1, table_names[i], table_types[i], table_size[i], i + 1, record, &recordSize); // not sure we actually need recordSize
        rc = catalog->insertRecord(columnHandle, columnDescriptor, record, rid);
        if (rc != SUCCESS)
        {
            cerr << "Unable to insert \"Table\" record number " << i << " into table Column" << endl;
            return rc;
        }
    }

    string column_names[5] = {"table-id", "column-name", "column-type", "column-length", "column-position"};
    AttrType column_types[5] = {TypeInt, TypeVarChar, TypeInt, TypeInt, TypeInt};
    int column_size[5] = {4, 50, 4, 4, 4};
    for (int i = 0; i < 5; i += 1)
    {
        prepareColumnRecord(columnDescriptor.size(), nullsIndicatorColumnString, 2, column_names[i], column_types[i], column_size[i], i + 1, record, &recordSize); // not sure we actually need recordSize
        rc = catalog->insertRecord(columnHandle, columnDescriptor, record, rid);
        if (rc != SUCCESS)
        {
            cerr << "Unable to insert \"Table\" record number " << i << " into table Column" << endl;
            return rc;
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
    if (catalog == NULL)
        return CATALOG_DSN_EXIST;

    catalog->closeFile(*tableHandle);
    catalog->closeFile(*columnHandle);
    catalog->destroyFile("Tables");
    catalog->destroyFile("Columns");

    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    // have to check if table exists already, only way I see is by going through the catalog to see if there is already a table of the same name (idk how to do that yet)
    RC rc;
    rc = catalog->createFile(tableName);
    if (rc != SUCCESS)
    {
        cerr << "Unable to create " << tableName << " file" << endl;
        return TABLE_FILE_ALR_EXISTS;
    }
    FileHandle handle;
    rc = catalog->openFile(tableName, handle);
    if (rc != SUCCESS)
    {
        cerr << "Unable to open " << tableName << " file" << endl;
        return rc;
    }

    // prep variables for record
    RID rid;
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);

    // prep variables for new table entry
    vector<Attribute> tableDescriptor;
    createTableRecordDescriptor(tableDescriptor);
    // despite no fields ever being null, we will create the nulls indicator
    int tableNullFieldsIndicatorActualSize = ceil((double)tableDescriptor.size() / CHAR_BIT);
    char nullsIndicatorTable[tableNullFieldsIndicatorActualSize];
    memset(nullsIndicatorTable, 0, tableNullFieldsIndicatorActualSize);
    string nullsIndicatorTableString(nullsIndicatorTable);

    // create "columns" table
    vector<Attribute> columnDescriptor;
    createColumnRecordDescriptor(columnDescriptor);
    // despite no fields ever being null, we will create the nulls indicator
    int columnNullFieldsIndicatorActualSize = ceil((double)columnDescriptor.size() / CHAR_BIT);
    char nullsIndicatorColumn[columnNullFieldsIndicatorActualSize];
    memset(nullsIndicatorColumn, 0, columnNullFieldsIndicatorActualSize);
    string nullsIndicatorColumnString(nullsIndicatorColumn);

    prepareTableRecord(tableDescriptor.size(), nullsIndicatorTableString, table_id_count, tableName, tableName, record, &recordSize);
    rc = catalog->insertRecord(handle, tableDescriptor, record, rid);
    if (rc != SUCCESS)
    {
        cerr << "Unable to insert " << tableName << " record into table Table" << endl;
        return rc;
    }

    int num_attrs = attrs.size();
    for (int i = 0; i < num_attrs; i += 1)
    {
        prepareColumnRecord(columnDescriptor.size(), nullsIndicatorColumnString, 2, attrs[i].name, attrs[i].type, attrs[i].length, i + 1, record, &recordSize); // not sure we actually need recordSize
        rc = catalog->insertRecord(*columnHandle, columnDescriptor, record, rid);
        if (rc != SUCCESS)
        {
            cerr << "Unable to insert " << tableName << " record number " << i << " into table Column" << endl;
            return rc;
        }
    }

    table_id_count += 1;
    catalog->closeFile(handle);
    free(record);
    free(returnedData);

    return SUCCESS;
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
    // Check if catalog has been created
    if (catalog == NULL)
    {
        return CATALOG_DSN_EXIST;
    }

    // Check if table file exists
    FileHandle tableFileHandle;
    RC rc = catalog->openFile(table, tableFileHandle);
    if (rc != SUCCESS)
    {
        return rc;
    }

    // Prepare to scan the Tables file
    RBFM_ScanIterator tablesScanIterator;
    vector<string> tablesAttributesToRead = {"table-id", "table-name"};
    vector<Attribute> tableDescriptor;
    createTableRecordDescriptor(tableDescriptor);
    string conditionAttribute = "table-name";
    CompOp compOp = EQ_OP;
    void *value = (void *)tableName.c_str();

    // Create scan iterator
    rc = catalog->scan(tableFileHandle, tableDescriptor, conditionAttribute, compOp, value, tablesAttributesToRead, tablesScanIterator);
    if (rc != SUCCESS)
    {
        catalog->closeFile(tableFileHandle);
        return 3;
    }

    // Use iterator to iterate through table file to find desired table
    RID rid;
    void *data = malloc(PAGE_SIZE);
    int tableID = -1;
    bool found = false;
    while (tablesScanIterator.getNextRecord(rid, data) != RBFM_EOF)
    {
        int offset = int(ceil((double)tableDescriptor.size() / CHAR_BIT)); // Have to account for empty nullIndicator
        memcpy(&tableID, (char *)data + offset, sizeof(int));              // Grabs tableID
        found = true;
        break; // Assuming table names are unique, we can break after the first match
    }
    tablesScanIterator.close();
    catalog->closeFile(tableFileHandle);

    // If table does not exist
    if (!found)
    {
        free(data);
        return 4;
    }

    // Access the Columns file
    FileHandle columnFileHandle;
    rc = catalog->openFile("Columns", columnFileHandle);
    if (rc != SUCCESS)
    {
        free(data);
        return 5;
    }

    // Prepare to scan the Columns file
    vector<string> columnsAttributesToRead = {"column-name", "column-type", "column-length"};
    vector<Attribute> columnDescriptor;
    createColumnRecordDescriptor(columnDescriptor);
    RBFM_ScanIterator columnsScanIterator;
    int tableIdValue = tableID;
    value = &tableIdValue;

    // Create scan iterator
    rc = catalog->scan(columnFileHandle, columnDescriptor, "table-id", compOp, value, columnsAttributesToRead, columnsScanIterator);
    if (rc != SUCCESS)
    {
        catalog->closeFile(columnFileHandle);
        free(data);
        return 6;
    }

    // Iterates through Columns table and adds matched attributes corresponding to tableID
    Attribute attr;
    while (columnsScanIterator.getNextRecord(rid, data) != RBFM_EOF)
    {
        int offset = int(ceil((double)columnDescriptor.size() / CHAR_BIT)); // Offset accounting for empty nullindicator

        // Grabs name of attribute
        int nameLen;
        memcpy(&nameLen, (char *)data + offset, sizeof(int));
        offset += sizeof(int);
        char *name = (char *)malloc(nameLen + 1);
        memcpy(name, (char *)data + offset, nameLen);
        name[nameLen] = '\0';
        attr.name = string(name);
        offset += nameLen;

        // Grabs type of attribute
        memcpy(&attr.type, (char *)data + offset, sizeof(int));
        offset += sizeof(int);

        // Grabs length of type
        memcpy(&attr.length, (char *)data + offset, sizeof(AttrLength));
        attrs.push_back(attr);
        free(name);
    }
    columnsScanIterator.close();
    catalog->closeFile(columnFileHandle);
    free(data);
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    if (catalog == NULL)
    {
        return CATALOG_DSN_EXIST;
    }

    FileHandle tableFileHandle;
    RC rc = catalog->openFile(tableName, tableFileHandle);
    if (rc != SUCCESS)
    {
        return rc; // Failed to open the file
    }

    vector<Attribute> attrs;
    getAttributes(tableName, attrs);

    rc = catalog->insertRecord(tableFileHandle, attrs, data, rid);
    catalog->closeFile(tableFileHandle);
    if (rc != SUCCESS)
    {
        return rc; // Return error if the insertion fails
    }
    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    if (catalog == NULL)
    {
        return CATALOG_DSN_EXIST;
    }
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    if (catalog == NULL)
    {
        return CATALOG_DSN_EXIST;
    }
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    if (catalog == NULL)
    {
        return CATALOG_DSN_EXIST;
    }
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
void RelationManager::prepareTableRecord(const int nameLength, const string &name, const int table_id, const string &table_name, const string &file_name, void *buffer, int *recordSize)
{
    int offset = 0;

    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;

    memcpy((char *)buffer + offset, &table_id, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, table_name.c_str(), table_name.size() + 1); // plus 1 for null terminator
    offset += table_name.size() + 1;

    memcpy((char *)buffer + offset, file_name.c_str(), file_name.size() + 1); // plus 1 for null terminator
    offset += file_name.size() + 1;

    *recordSize = offset;
}

void RelationManager::prepareColumnRecord(const int nameLength, const string &name, const int table_id, const string column_name, const int column_type,
                                          const int column_length, const int column_position, void *buffer, int *recordSize)
{
    int offset = 0;

    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;

    memcpy((char *)buffer + offset, &table_id, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, column_name.c_str(), column_name.size() + 1);
    offset += column_name.size() + 1;

    memcpy((char *)buffer + offset, &column_type, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &column_length, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &column_position, sizeof(int));
    offset += sizeof(int);

    *recordSize = offset;
}

void RelationManager::createTableRecordDescriptor(vector<Attribute> &recordDescriptor)
{

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

void RelationManager::createColumnRecordDescriptor(vector<Attribute> &recordDescriptor)
{

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

// Method will scan table file based on the given file name and will place the file name within filename argument.
// Requires that tableDescriptor is already called with createTableRecordDescriptor beforehand
RC findTableFileName(const string &tableName, RecordBasedFileManager *rbfm, FileHandle &tableFileHandle,
                     const vector<Attribute> tableDescriptor, string &fileName)
{
    vector<string> attrsToRead = {"file-name"};

    // Setup scan iterator
    RBFM_ScanIterator rbfm_ScanIterator;
    string conditionAttribute = "table-name";
    CompOp compOp = EQ_OP;
    void *value = (void *)tableName.c_str();

    // Create Scan iterator
    RC rc = rbfm->scan(tableFileHandle, tableDescriptor, conditionAttribute, compOp, value, attrsToRead, rbfm_ScanIterator);
    if (rc != SUCCESS)
    {
        return rc;
    }

    RID rid;
    char data[PAGE_SIZE];
    if (rbfm_ScanIterator.getNextRecord(rid, &data) != RBFM_EOF)
    {
        int offset = ceil(static_cast<double>(attrsToRead.size()) / CHAR_BIT); // Skip over null indictors
        int length = *(int *)(data + offset);                                  // Grab the length of varchar
        fileName.assign(data + offset + sizeof(int), length);                  // assign places file name in the string
        rbfm_ScanIterator.close();
        return SUCCESS;
    }

    rbfm_ScanIterator.close();
    return TB_DN_EXIST; // Table was not found
}
