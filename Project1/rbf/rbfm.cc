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
    /* cerr << "insertRecord: Found: " << recordBytes << " null bytes." << endl; */
    short num_bytes_array = (sizeof(short) * recordDescriptor.size());

    //these will hold values of any field i from the range [start, end)
    short* field_end = (short *)calloc(recordDescriptor.size(), sizeof(short));
    short* field_start = (short *)calloc(recordDescriptor.size(), sizeof(short));
    memset(field_start, 0, num_bytes_array);
    memset(field_end, 0, num_bytes_array);
    // the size of any field will be calculated as field_end[i] - field_start[i]

    int byteOffset = 0;
    int bitOffset = 7;
    for (unsigned i = 0; i < recordDescriptor.size(); i++) {
        // Check if the NULL bit is valid.
        nullBit = (nullOffset[byteOffset] & (1 << bitOffset));
        if (!nullBit) {
            // The field an int.
            if (recordDescriptor[i].type == 0) {
                recordBytes += recordDescriptor[i].length;
                if (i == 0) {
                    field_start[i] = nullBytes + num_bytes_array;
                    field_end[i] = nullBytes + num_bytes_array + recordDescriptor[i].length;
                }
                else {
                    field_start[i] = field_end[i-1];
                    field_end[i] = recordDescriptor[i].length + field_end[i-1];
                }
                
            // The field is an float.
            } else if (recordDescriptor[i].type == 1){
                recordBytes += recordDescriptor[i].length;
                if (i == 0) {
                    field_start[i] = nullBytes + num_bytes_array;
                    field_end[i] = nullBytes + num_bytes_array + recordDescriptor[i].length;
                }
                else {
                    field_start[i] = field_end[i-1];
                    field_end[i] = recordDescriptor[i].length + field_end[i-1];
                }
            // The field is a varchar.
            } else if (recordDescriptor[i].type == 2) {
                short length = 0;
                memcpy(&length, &(((char*)data)[recordBytes]), sizeof(int));
                // memcpy(&length, (char *)data + recordBytes, sizeof(int));
                recordBytes += sizeof(int);
                recordBytes += length;
                if (i == 0) {
                    field_start[i] = nullBytes + num_bytes_array;
                    field_end[i] = nullBytes + num_bytes_array + length + sizeof(int);
                }
                else {
                    field_start[i] = field_end[i-1];
                    field_end[i] = length + sizeof(int) + field_end[i-1];
                }
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
    
    // We need 4 bytes for the directory entry.
    // first term is accounting for entry in directory, second term is the record itself, 3rd term is the size of the array that contains offsets of each field in record
    short totalBytesNeeded = (2 * sizeof(short)) + recordBytes + num_bytes_array; 

    if (totalBytesNeeded > PAGE_SIZE) {
        cerr << "insertRecord: Record is larger than page size." << endl;
        return -2;
    }

    char *pageData = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (pageData == NULL) {
        cerr << "insertRecord: Unable to allocate memory." << endl;
        return -1;
    }

    memset(pageData, 0, PAGE_SIZE);

    short freeSpaceOffset = 0;
    short numSlots = 0;
    unsigned directoryEntryOffset = 0;
    short bytesAvailable = 0;
    bool mustAppend = true;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (unsigned i = 0; (i < numPages) && mustAppend; i++) {
        directoryEntryOffset = 0;
        fileHandle.readPage(i, pageData);
        // The first two bytes indicate the offset to the end of free space in the file.
        memcpy(&freeSpaceOffset, &pageData[directoryEntryOffset], sizeof(short));
        directoryEntryOffset += sizeof(short);
        // The next two bytes indicate the number of slots in the page.
        memcpy(&numSlots, &pageData[directoryEntryOffset], sizeof(short));
        directoryEntryOffset += sizeof(short);
        // Calculate the number of available bytes.
        // The freeSpaceOffset and numSlots each take 2 bytes of the page, for a total of 4 bytes.
        // Each directory entry takes 4 bytes to store, so it takes (numSlots * 4) bytes to store the entries of the directory.
        bytesAvailable = freeSpaceOffset - (numSlots * 2 * sizeof(short)) - (2 * sizeof(short)) - 1;
        if (totalBytesNeeded <= bytesAvailable) {
            // Insert record starting at freeSpaceOffset - (recordBytes + sizeof(array of all offsets).
            short destinationOffset = freeSpaceOffset - (recordBytes + num_bytes_array) - 1;
            /* cerr << "insertRecord: record size: " << recordBytes << "\tfreeSpaceOffset: " << freeSpaceOffset << endl; */
            if (freeSpaceOffset - destinationOffset >= recordBytes) { //I dont understand why we need this if check
                // Copy in the NULLBYTES, then the array to each record field, followed by the record 
                memcpy(&pageData[destinationOffset], data, nullBytes);
                memcpy(&pageData[destinationOffset + nullBytes], field_start, num_bytes_array);
                memcpy(&pageData[destinationOffset + nullBytes + num_bytes_array], &(((char*)data)[nullBytes]), recordBytes - nullBytes);
            } else {
                cerr << "insertRecord: Unable to insert record data." << endl;
                continue;
            }
            // Copy in the pointer to new end of free space.
            memcpy(&pageData[0], &destinationOffset, sizeof(short));
            // Update the total number of slots.
            // We need 4 bytes per entry and 4 bytes for the directory header.
            directoryEntryOffset = (numSlots * 2 * sizeof(short)) + (2 * sizeof(short));
            // Store the offset of the record.

            // cerr << "destination offset: " << destinationOffset << endl;
            // cerr << "directory entry offset: " << directoryEntryOffset << endl;
            // cerr << "total necessary bytes: " << totalBytesNeeded << endl;
            // cerr << "available bytes: " << bytesAvailable << endl;
            if (destinationOffset - directoryEntryOffset >= (2 * sizeof(short))) {
                memcpy(&pageData[directoryEntryOffset], &destinationOffset, sizeof(short));
                directoryEntryOffset += sizeof(short);
                // Store the length of the record.
                short len_of_record_stored = recordBytes + (sizeof(short) * recordDescriptor.size());
                memcpy(&pageData[directoryEntryOffset], &len_of_record_stored, sizeof(short)); 
            } else {
                cerr << "insertRecord: Unable to update data page directory." << endl;
                free(pageData);
                return -1;
            }
            numSlots++;
            memcpy(&pageData[sizeof(short)], &numSlots, sizeof(short));
            // if ((numSlots == 2) && (i == 0)) {
            //     cerr << "Found a new RID of 0, 2." << endl;
            // }

            // Update the rid.
            rid.pageNum = i;
            rid.slotNum = numSlots;

            // Copy in the new number of records.
            // I think &numSlots is the only way to do this, but this may cause memory issues later.
            memcpy(&pageData[sizeof(short)], &numSlots, sizeof(short));
            RC retValue = fileHandle.writePage(i, pageData);
            if (retValue != 0) {
                cerr << "insertRecord: Unable to write page to page number: " << i << endl;
                free(pageData);
                return -4;
            }
            mustAppend = false;

            break;
        }
    }
    
    if (mustAppend) {
        memset(pageData, 0, PAGE_SIZE);
        freeSpaceOffset = PAGE_SIZE - recordBytes - num_bytes_array - 1;
        // Copy in the record.
        if (PAGE_SIZE - freeSpaceOffset >= recordBytes + num_bytes_array) {
            // memcpy(&pageData[freeSpaceOffset], data, recordBytes);
            // cerr << "append page insert record info" << endl;
            // cerr << "size of array = " << num_bytes_array << endl;
            // cerr << "putting null bytes at: " << freeSpaceOffset << endl;
            // cerr << "putting array at " << (freeSpaceOffset + nullBytes) << endl;
            // cerr << "putting the rest of the record at " << (freeSpaceOffset + nullBytes + num_bytes_array) << endl;
            memcpy(&pageData[freeSpaceOffset], data, nullBytes);
            memcpy(&pageData[freeSpaceOffset + nullBytes], field_start, num_bytes_array);
            memcpy(&pageData[freeSpaceOffset + nullBytes + num_bytes_array], &(((char*)data)[nullBytes]), recordBytes - nullBytes);


            // memcpy(&pageData[freeSpaceOffset], field_start, (sizeof(short) * recordDescriptor.size()));
            // memcpy(&pageData[freeSpaceOffset + (sizeof(short) * recordDescriptor.size())], data, recordBytes);
        } else {
            cerr << "insertRecord: Record too large." << endl;
            free(field_start);
            free(field_end);
            free(pageData);
            return -6;
        }

        if ((unsigned)freeSpaceOffset >= (4 * sizeof(short))) {
            // Copy in the pointer to free space.
            directoryEntryOffset = 0;
            memcpy(&pageData[directoryEntryOffset], &freeSpaceOffset, sizeof(short));
            directoryEntryOffset += sizeof(short);
            numSlots = 1;
            // Copy in the number of records.
            memcpy(&pageData[directoryEntryOffset], &numSlots, sizeof(short));
            /* cerr << "insertRecord: numslots: " << numSlots << endl; */
            directoryEntryOffset += sizeof(short);
            // Store the offset of the record.
            memcpy(&pageData[directoryEntryOffset], &freeSpaceOffset, sizeof(short));
            directoryEntryOffset += sizeof(short);
            // Store the length of the record.
            short len_of_record_stored = recordBytes + (sizeof(short) * recordDescriptor.size());
            memcpy(&pageData[directoryEntryOffset], &len_of_record_stored, sizeof(short)); 
        } else {
            cerr << "insertRecord: Error updating page directory." << endl;
            free(field_start);
            free(field_end);
            free(pageData);
            return -3;
        }

        fileHandle.appendPage(pageData);

        // Update the rid.
        rid.pageNum = numPages;
        rid.slotNum = 1;
    }

    free(field_start);
    free(field_end);
    free(pageData);

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    unsigned pageNum = rid.pageNum;
    unsigned slotNum = rid.slotNum;

    // Determine the record size.
    int nullBytes = ceil((double) recordDescriptor.size() / CHAR_BIT);

    //these will hold values of any field i from the range [start, end)
    short* attribute_offsets = (short *)calloc(recordDescriptor.size(), sizeof(short));

    char* pageData = (char *)malloc(PAGE_SIZE);
    memset(pageData, 0, PAGE_SIZE);
    RC retValue = fileHandle.readPage(pageNum, pageData);
    if (retValue != 0) {
        cerr << "readRecord: Unable to read page." << endl;
        free(pageData);
        free(attribute_offsets);
        return -1;
    }

    // Check if the slotNum is valid.
    short totalSlotsUsed = 0;
    
    memcpy(&totalSlotsUsed, &pageData[sizeof(short)], sizeof(short));
    if (slotNum <= (unsigned)totalSlotsUsed) {
        short recordOffset = 0;
        short recordSize = 0;
        short pageDataOffset = (slotNum * (2 * sizeof(short)));

        // get where the record is on the page
        memcpy(&recordOffset, &pageData[pageDataOffset], sizeof(short));
        // adjust offset to get the size of the record next
        pageDataOffset += sizeof(short);
        //get the size of the entire record (null bits, array to jump to field, actual record)
        memcpy(&recordSize, &pageData[pageDataOffset], sizeof(short));

        //copy in the nullBytes
        memcpy(data, &pageData[recordOffset], nullBytes);
        //copy our array to jump to field
        recordOffset += nullBytes;
        memcpy(attribute_offsets, &pageData[recordOffset], recordDescriptor.size() * sizeof(short));
        //copy the rest of the record to data
        recordOffset += recordDescriptor.size() * sizeof(short);
        memcpy(&(((char*)data)[nullBytes]), &pageData[recordOffset], recordSize - (recordDescriptor.size() * sizeof(short)) - nullBytes);

        // memcpy(data, &pageData[recordOffset], recordSize);
    } else {
        free(pageData);
        free(attribute_offsets);
        return -1;
    }

    free(pageData);
    free(attribute_offsets);
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
