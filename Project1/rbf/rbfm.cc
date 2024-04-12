#include <cstdio>
#include <iostream>
#include <cstring>
#include <cmath>

#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    PagedFileManager *pfm = PagedFileManager::instance();
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    PagedFileManager *pfm = PagedFileManager::instance();
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    PagedFileManager *pfm = PagedFileManager::instance();
    return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    PagedFileManager *pfm = PagedFileManager::instance();
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    /* PagedFileManager *pfm = PagedFileManager::instance(); */
    /* return _pf_manager.insertRecord(fileHandle, recordDescriptor, data, rid); */
    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    /* PagedFileManager *pfm = PagedFileManager::instance(); */
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    int nullBytes = ceil((double) recordDescriptor.size() / CHAR_BIT);
    bool nullBit = false;
    // Initialize a pointer to the first byte of the NULL bits.
    char* nullOffset = ((char *) data);

    // Initialize a pointer to the first field in the record, which is after the NULL bytes..
    char* fields = ((char *) data);
    fields += nullBytes;

    int byteOffset = 0;
    int bitOffset = 7;
    for (unsigned i = 0; i < recordDescriptor.size(); i++) {
        cout << recordDescriptor[i].name << ": ";
        // Check if the NULL bit is valid.
        nullBit = (nullOffset[byteOffset] & (1 << bitOffset));
        if (!nullBit) {
            // The field an int.
            if (recordDescriptor[i].type == 0) {
                int curr;
                memcpy(&curr, fields, recordDescriptor[i].length);
                fields += recordDescriptor[i].length;

                cout << curr << "\t";
            // The field is an float.
            } else if (recordDescriptor[i].type == 1){
                float curr;
                memcpy(&curr, fields, recordDescriptor[i].length);
                fields += recordDescriptor[i].length;

                cout << curr << "\t";
            // The field is a varchar.
            } else if (recordDescriptor[i].type == 2) {
                int length;
                memcpy(&length, fields, 4);
                fields += 4;

                char str[length + 1];
                memcpy(&str, fields, length);
                // Must add NULL terminating bit to string manually!
                str[length] = '\0';
                fields += length;

                cout << std::string(str) << "\t";
            } else {
                cerr << endl << "printRecord: Invalid record descriptor type." << endl;
                return 1;
            }
        } else {
            cout << "NULL\t";
        }

        bitOffset--;
        if (bitOffset < 0) {
            bitOffset = 7;
            byteOffset++;
        }
    }
    cout << endl;
    return 0;
}
