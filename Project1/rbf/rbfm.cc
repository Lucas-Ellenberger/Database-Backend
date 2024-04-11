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
    if (_pf_manager == 0) {
        cerr << "printRecord: _pf_manager has not been initialized.\n" << endl;
        return 1;
    }

    num_records = recordDescriptor.size();
    n = ceil(num_records / 8);
    // Initialize a pointer to the first bit of the NULL bits.
    null_ptr = data;
    // Initialize a pointer to the first byte of real data, which is after the n NULL bytes.
    data_ptr = data + (n * sizeof(byte));

    vector<Attribute>::iterator it;
    int index = 0;
    bool valid;
    for (it=recordDescriptor.begin(); it!=recordDescriptor.end(); it++) {
        // Check if the NULL bit is valid.
        valid = (null_ptr & (1 << index))
        if (recordDescriptor.type == 0) {  
            cout << recordDescriptor.Name << ": " << 
        } else if (recordDescriptor.type == 1) {

        } else if (recordDescriptor.type == 2) {

        } else {
            fprintf(stderr, "Invalid record descriptor type.\n");
            return 1;
        }
        index++;
    }
    return -1;
}
