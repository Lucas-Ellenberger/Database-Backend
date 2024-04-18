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
    // Determine the record size.
    int nullBytes = ceil((double) recordDescriptor.size() / CHAR_BIT);
    bool nullBit = false;
    // Initialize a pointer to the first byte of the NULL bits.
    char* nullOffset = ((char *) data);

    short recordBytes = nullBytes;
    cerr << "insertRecord: Found: " << recordBytes << " null bytes." << endl;

    int byteOffset = 0;
    int bitOffset = 7;
    for (unsigned i = 0; i < recordDescriptor.size(); i++) {
        // Check if the NULL bit is valid.
        nullBit = (nullOffset[byteOffset] & (1 << bitOffset));
        if (!nullBit) {
            // The field an int.
            if (recordDescriptor[i].type == 0) {
                recordBytes += recordDescriptor[i].length;
                cerr << "Found an int so our new length is: " << recordBytes << endl;
            // The field is an float.
            } else if (recordDescriptor[i].type == 1){
                recordBytes += recordDescriptor[i].length;
                cerr << "Found a float so our new length is: " << recordBytes << endl;
            // The field is a varchar.
            } else if (recordDescriptor[i].type == 2) {
                recordBytes += 4;
                cerr << "strlen() found this length: " << strlen((char *)data + recordBytes) << endl;
                short strLength = strlen((char *)data + recordBytes);
                // We ignore the null byte in the string.
                recordBytes += strLength - 1;

                cerr << "Found a var char so our new length is: " << recordBytes << endl;
            } else {
                cerr << endl << "insertRecord: Invalid record descriptor type." << endl;
                return 1;
            }
        }

        bitOffset--;
        if (bitOffset < 0) {
            bitOffset = 7;
            byteOffset++;
        }
    }
    cerr << "insertRecord: We need: " << recordBytes << " bytes." << endl;

    // We need 4 bytes for the directory entry.
    short totalBytesNeeded = 4 + recordBytes;

    char *pageData = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (pageData == NULL) {
        cerr << "insertRecord: Unable to allocate memory." << endl;
        return -1;
    }

    memset(pageData, 0, PAGE_SIZE);
    short freeSpaceOffset = 0;
    short numSlots = 0;
    short directoryEntryOffset = 0;
    short bytesAvailable = 0;
    bool mustAppend = true;
    unsigned numPages = fileHandle.getNumberOfPages();
    /* for (unsigned i = 0; (i < numPages) && mustAppend; i++) { */
    /*     fileHandle.readPage(i, pageData); */
    /*     // The first two bytes indicate the offset to the end of free space in the file. */
    /*     memcpy(&freeSpaceOffset, pageData, 2); */
    /*     // The next two bytes indicate the number of slots in the page. */
    /*     memcpy(&numSlots, (char *)pageData + 2, 2); */
    /*     // Calculate the number of available bytes. */
    /*     // The freeSpaceOffset and numSlots each take 2 bytes of the page, for a total of 4 bytes. */
    /*     // Each directory entry takes 4 bytes to store, so it takes (numSlots * 4) bytes to store the entries of the directory. */
    /*     bytesAvailable = freeSpaceOffset - (numSlots * 4) - 4; */
    /*     if (totalBytesNeeded <= bytesAvailable) { */
    /*         // Insert record starting at freeSpaceOffset - recordBytes. */
    /*         short destinationOffset = freeSpaceOffset - recordBytes; */
    /*         cerr << "insertRecord: record size: " << recordBytes << "\tfreeSpaceOffset: " << freeSpaceOffset << endl; */
    /*         if (freeSpaceOffset - destinationOffset >= recordBytes) { */
    /*             // Copy in the record. */
    /*             memcpy(&pageData[destinationOffset], data, recordBytes); */
    /*         } else { */
    /*             cerr << "insertRecord: Unable to insert record data." << endl; */
    /*             continue; */
    /*         } */
    /*         // Copy in the pointer to new end of free space.. */
    /*         memcpy(&pageData[0], &destinationOffset, 2); */
    /*         // We need 4 bytes per entry and 4 bytes for the directory header. where directory header is just telling us free space offset and numSlots */
    /*         directoryEntryOffset = (numSlots * 4) + 4; */
    /*         // Store the offset of the record. */
    /*         if (destinationOffset - directoryEntryOffset >= 4) { */
    /*             memcpy(&pageData[directoryEntryOffset], &freeSpaceOffset, 2); */
    /*             directoryEntryOffset += 2; */
    /*             // Store the length of the record. */
    /*             memcpy(&pageData[directoryEntryOffset], &recordBytes, 2); */
    /*         } else { */
    /*             cerr << "insertRecord: Unable to update data page directory." << endl; */
    /*             free(pageData); */
    /*             return -1; */
    /*         } */

    /*         // Update the rid. */
    /*         rid.pageNum = i; */
    /*         rid.slotNum = numSlots; */

    /*         numSlots++; */
    /*         // Copy in the new number of records. */
    /*         // I think &numSlots is the only way to do this, but this may cause memory issues later. */
    /*         memcpy((char *)pageData + 2, &numSlots, 2); */
    /*         mustAppend = false; */

    /*         break; */
    /*     } */
    /* } */

    if (mustAppend) {
        memset(pageData, 0, PAGE_SIZE);
        fileHandle.appendPage(pageData);
        freeSpaceOffset = PAGE_SIZE - recordBytes;
        cerr << "insertRecord: record size: " << recordBytes << "\tfreeSpaceOffset: " << freeSpaceOffset << endl;
        // Copy in the record.
        if (PAGE_SIZE - freeSpaceOffset >= recordBytes) {
            memcpy(&pageData[freeSpaceOffset], data, recordBytes);
            fileHandle.appendPage(pageData);
        } else {
            cerr << "insertRecord: Record too large." << endl;
        }
        cerr << "insertRecord: data: " << data << endl;
        /* directoryEntryOffset = 4; */
        if (freeSpaceOffset >= 8) {
            // Copy in the pointer to free space.
            memcpy(&pageData[0], &freeSpaceOffset, 2);
            fileHandle.appendPage(pageData);
            numSlots = 1;
            // Copy in the number of records.
            memcpy(&pageData[2], &numSlots, 2);
            fileHandle.appendPage(pageData);
            // Store the offset of the record.
            memcpy(&pageData[4], &freeSpaceOffset, 2);
            fileHandle.appendPage(pageData);
            /* directoryEntryOffset += 2; */
            // Store the length of the record.
            cerr << "insertRecord: recordBytes: " << recordBytes << endl;
            memcpy(&pageData[6], &recordBytes, 2);
            fileHandle.appendPage(pageData);
        }

        fileHandle.appendPage(pageData);

        // Update the rid.
        rid.pageNum = numPages;
        rid.slotNum = 0;
    }

    free(pageData);

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    unsigned pageNum = rid.pageNum;
    unsigned slotNum = rid.slotNum;

    void* pageData = malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum, pageData);

    // Check if the slotNum is valid.
    unsigned totalSlotsUsed = 0;
    
    memcpy(&totalSlotsUsed, ((char*)pageData) + 2, 2);
    if (slotNum < totalSlotsUsed) {
        short recordOffset = 0;
        short recordSize = 0;
        // Add 4 bytes for the totalSlotNum and freeSpaceOffset, multiply by 4 to get to start of directory entry.
        memcpy(&recordOffset, (char*)pageData + ((slotNum*4)+4), 2);
        cerr << "readRecord: found a record offset of: " << recordOffset << endl;
        memcpy(&recordSize, (char*)pageData + ((slotNum*4)+4), 2);
        memcpy(data, (char*)pageData + recordOffset, recordSize);
    } else {
        cerr << "slot number is not in use or otherwise invalid" << endl;
        return -1;
    }

    free(pageData);
    return 0;
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
                int curr = 0;
                memcpy(&curr, fields, recordDescriptor[i].length);
                fields += recordDescriptor[i].length;

                cout << curr << "\t";
            // The field is an float.
            } else if (recordDescriptor[i].type == 1){
                float curr = 0;
                memcpy(&curr, fields, recordDescriptor[i].length);
                fields += recordDescriptor[i].length;

                cout << curr << "\t";
            // The field is a varchar.
            } else if (recordDescriptor[i].type == 2) {
                short length = 0;
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
