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
    if (_pf_manager == 0) {
        cerr << "createFile: _pf_manager has not been initialized.\n" << endl;
        return 1;
    }
    return _pf_manager.createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    if (_pf_manager == 0) {
        cerr << "destroyFile: _pf_manager has not been initialized.\n" << endl;
        return 1;
    }
    return _pf_manager.destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    if (_pf_manager == 0) {
        cerr << "openFile: _pf_manager has not been initialized.\n" << endl;
        return 1;
    }
    return _pf_manager.openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    if (_pf_manager == 0) {
        cerr << "closeFile: _pf_manager has not been initialized.\n" << endl;
        return 1;
    }
    return _pf_manager.closeFile(FileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    if (_pf_manager == 0) {
        cerr << "insertRecord: _pf_manager has not been initialized.\n" << endl;
        return 1;
    }
    return _pf_manager.insertRecord(fileHandle, recordDescriptor, data, rid);
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    if (_pf_manager == 0) {
        cerr << "readRecord: _pf_manager has not been initialized.\n" << endl;
        return 1;
    }
    return _pf_manager;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    /* PagedFileManager *pfm = PagedFileManager::instance(); */
    int nullBytes = ceil((double) recordDescriptor.size() / CHAR_BIT);
    bool nullBit = false;
    // Initialize a pointer to the first byte of real data, which is after the n NULL bytes.
    int dataOffset = nullBytes;

    // Initialize a pointer to the first bit of the NULL bits.
    const char* nullOffset = (char *) data;

    int byteOffset = 0;
    int bitOffset = 7;
    for (unsigned i = 0; i < recordDescriptor.size(); i++) {
        cout << recordDescriptor[i].name << ":\t";
        // Check if the NULL bit is valid.
        nullBit = (nullOffset[byteOffset] & (1 << bitOffset));
        if (!nullBit) {
            // The field in the record is either an int or float.
            if (recordDescriptor[i].type == 0) {
                int curr;
                memcpy(&curr, data[dataOffset], sizeof(recordDescriptor[i].type));
                dataOffset += recordDescriptor[i].length;

                cout << curr << "\t";
            } else if (recordDescriptor[i].type == 1){
                float curr;
                memcpy(&curr, dataOffset, sizeof(recordDescriptor[i].type));
                dataOffset += recordDescriptor[i].length;

                cout << curr << "\t";
            } else if (recordDescriptor[i].type == 2) {
                int length;
                memcpy(&length, dataOffset, sizeof(int));
                dataOffset += sizeof(int);

                char str[recordDescriptor[i].length];
                memcpy(&str, dataOffset, recordDescriptor[i].length);
                dataOffset += recordDescriptor[i].length;

                cout << str << "\t";
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
