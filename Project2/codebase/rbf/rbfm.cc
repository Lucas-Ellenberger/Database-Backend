#include "rbfm.h"
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager *RecordBasedFileManager::instance()
{
    if (!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName)
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void *firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);
    // Adds the first record based page.

    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName)
{
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle)
{
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;

    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if (!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting up the return RID.
    rid.pageNum = i;
    bool foundSlot = false;
    SlotDirectoryRecordEntry newRecordEntry;
    for (uint32_t slot = 0; slot < slotHeader.recordEntriesNumber; slot += 1)
    {
        newRecordEntry = getSlotDirectoryRecordEntry(pageData, slot);
        if (newRecordEntry.length < 0)
        {
            foundSlot = true;
            rid.slotNum = slot;
            break;
        }
    }

    if (!foundSlot)
        rid.slotNum = slotHeader.recordEntriesNumber;

    // Adding the new record reference in the slot directory.
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset(pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
    // Retrieve the specified page
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;

    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page.
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if (slotHeader.recordEntriesNumber <= rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data.
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    if (recordEntry.length < 0)
    {
        free(pageData);
        return RBFM_SLOT_ALR_DELETED;
    }

    if (recordEntry.offset < 0)
    {
        // This is a forwarding address.
        RID newRID;
        newRID.pageNum = (recordEntry.offset * -1);
        newRID.slotNum = recordEntry.length;
        free(pageData);
        return readRecord(fileHandle, recordDescriptor, newRID, data);
    }

    // Retrieve the actual entry data
    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
    // Parse the null indicator and save it into an array.
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);

    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned)recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
        case TypeInt:
            uint32_t data_integer;
            memcpy(&data_integer, ((char *)data + offset), INT_SIZE);
            offset += INT_SIZE;

            cout << "" << data_integer << endl;
            break;
        case TypeReal:
            float data_real;
            memcpy(&data_real, ((char *)data + offset), REAL_SIZE);
            offset += REAL_SIZE;

            cout << "" << data_real << endl;
            break;
        case TypeVarChar:
            // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
            uint32_t varcharSize;
            memcpy(&varcharSize, ((char *)data + offset), VARCHAR_LENGTH_SIZE);
            offset += VARCHAR_LENGTH_SIZE;

            // Gets the actual string.
            char *data_string = (char *)malloc(varcharSize + 1);
            if (data_string == NULL)
                return RBFM_MALLOC_FAILED;
            memcpy(data_string, ((char *)data + offset), varcharSize);

            // Adds the string terminator.
            data_string[varcharSize] = '\0';
            offset += varcharSize;

            cout << data_string << endl;
            free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
    // Retrieve the specified page
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;

    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    if (slotHeader.recordEntriesNumber <= rid.slotNum)
    {
        free(pageData);
        return RBFM_SLOT_DN_EXIST;
    }

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    if (recordEntry.length < 0)
    {
        // This means record has been deleted.
        free(pageData);
        return RBFM_SLOT_ALR_DELETED;
    }

    if (recordEntry.offset < 0)
    {
        // This is a forwarding address.
        RID newRID;
        newRID.pageNum = (recordEntry.offset * -1); // we need to be very careful about how we are defining the record offset.
                                                    // If it is negative, we can either just set the final bit to 1, or we can multiply by -1.
                                                    // these are likely not equivalent b/c Two's complement. So like... pick one
        newRID.slotNum = recordEntry.length;        // we shouldnt need to screw with the length, we just need to make sure that the MSB is only used
                                                    // to indicate whether something is deleted, and nothing more at all. If its forwarding address,
                                                    // the value can be positive (MSB is 0)
        deleteRecord(fileHandle, recordDescriptor, newRID);
        recordEntry.length = -1;
        recordEntry.offset = 0;
        setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);

        if (fileHandle.writePage(rid.pageNum, pageData))
            return RBFM_WRITE_FAILED;

        free(pageData);
        return SUCCESS;
    }
    // The entry is neither a forwarding address nor already deleted, meaning we have to delete it.

    // Delete the record at the entry.
    // to make this simple, we can set everything at that memory to 0, though it should be completely unnecessary (just doing it for safety)
    memset((char *)pageData + recordEntry.offset, 0, recordEntry.length); // The way the offset is currently handled will almost certainly need to be changed

    // Shift over the record data by the length of the old record.
    // Loop over every record entry in the slot directory.
    // If the record offset was shifted (the starting offset is less than the one we deleted):
    // Then, add the length of the deleted record.
    unsigned shiftBeginning = sizeof(SlotDirectoryHeader) + slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry);
    unsigned shiftSize = recordEntry.offset - shiftBeginning;

    memmove((char *)pageData + shiftBeginning + recordEntry.length, (char *)pageData + shiftBeginning, shiftSize);
    // Zero out the the where the data used to be.
    memset((char *)pageData + shiftBeginning, 0, recordEntry.length);

    for (uint32_t i = 0; i < slotHeader.recordEntriesNumber; i += 1)
    {
        SlotDirectoryRecordEntry next = getSlotDirectoryRecordEntry(pageData, i); // get the next record in order that are after the deleted record
        if (next.offset < recordEntry.offset)
        {
            next.offset += recordEntry.length;
            setSlotDirectoryRecordEntry(pageData, i, next);
        }
    }

    // modify directory to reflect that the entry has been deleted
    recordEntry.length = -1;
    recordEntry.offset = 0;
    // write it back
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);

    // Write back the page data
    if (fileHandle.writePage(rid.pageNum, pageData))
        return RBFM_WRITE_FAILED;

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
    // Retrieve the specified page
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;

    if (fileHandle.readPage(rid.pageNum, pageData))
    {
        free(pageData);
        return RBFM_READ_FAILED;
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    // Checks if the specific slot id exists in the page
    if (slotHeader.recordEntriesNumber <= rid.slotNum)
    {
        free(pageData);
        return RBFM_SLOT_DN_EXIST;
    }

    SlotDirectoryRecordEntry oldEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    // If forwarded: call delete address on forwarded address.
    // Then, call insert record to place it in a new slot / page.
    // Update the forwarding address to reflect the change.

    // Note: If we always delete and insert to a single new forward address:
    // Then, we should never create chains of forwarding addresses.

    // Update the offset and length in the corresponding slot directory.
    // DO NOT change the number of entries in the SlotDirectoryHeader.

    // Check if record has been deleted
    if (oldEntry.length < 0)
    {
        free(pageData);
        return RBFM_SLOT_ALR_DELETED;
    };

    // Check if oldEntry is a forwarding address
    if (oldEntry.offset < 0)
    {
        // Creates RID to be passed in deleteRecord
        RID forwardingRid;
        forwardingRid.pageNum = oldEntry.offset * -1;
        forwardingRid.slotNum = oldEntry.length;

        if (deleteRecord(fileHandle, recordDescriptor, forwardingRid))
        {
            free(pageData);
            return RBFM_DELETE_FAILED;
        }

        if (insertRecord(fileHandle, recordDescriptor, data, forwardingRid)) // After inserting, forwardingRid is updated with new rid
        {
            free(pageData);
            return RBFM_INSERT_FAILED;
        }

        // Preps RecordEntry to be inserted in directory
        SlotDirectoryRecordEntry newEntry;
        newEntry.offset = forwardingRid.pageNum * -1; // Multiply by -1 to set forwarding flag
        newEntry.length = forwardingRid.slotNum;      // Sets slot num of forwarded address

        // Update Slot Directory
        setSlotDirectoryRecordEntry(pageData, rid.slotNum, newEntry); // Accesses old rid for slot number

        // Write back the page data
        if (fileHandle.writePage(rid.pageNum, pageData))
        {
            free(pageData);
            return RBFM_WRITE_FAILED;
        }

        free(pageData);
        return SUCCESS;
    }

    // Case when record is present on the page
    if (deleteRecord(fileHandle, recordDescriptor, rid))
    {
        free(pageData);
        return RBFM_DELETE_FAILED;
    }

    // Creates RID to be passed in insertRecord
    RID forwardingRid;

    if (insertRecord(fileHandle, recordDescriptor, pageData, forwardingRid)) // After inserting, new Rid is stored in forwardingRid
    {
        free(pageData);
        return RBFM_INSERT_FAILED;
    }

    // Preps RecordEntry to be inserted in directory
    SlotDirectoryRecordEntry newEntry;
    newEntry.offset = forwardingRid.pageNum * -1; // Multiply by -1 to set forwarding flag
    newEntry.length = forwardingRid.slotNum;      // Sets slot num of forwarded address

    // Update RecordEntry to have forwarding address
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newEntry); // Accesses old rid for slot number

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
    // Retrieve the specified page
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;

    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    if (slotHeader.recordEntriesNumber <= rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    if (recordEntry.length < 0)
    {
        free(pageData);
        return RBFM_SLOT_ALR_DELETED;
    }

    if (recordEntry.offset < 0)
    {
        RID forwardingRid;
        forwardingRid.pageNum = recordEntry.offset * -1;
        forwardingRid.slotNum = recordEntry.length;
        free(pageData);
        return readAttribute(fileHandle, recordDescriptor, forwardingRid, attributeName, data);
    }

    // Points to start of record
    char *start = (char *)pageData + recordEntry.offset;

    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy(&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy(nullIndicator, (char *)pageData, nullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i + 1) / CHAR_BIT;
        int indicatorMask = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    // TODO: We only need to write out one null indicator bit for the one attribute!
    /* memcpy(data, nullIndicator, nullIndicatorSize); */

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (recordDescriptor[i].name == attributeName)
        {
            // TODO: Write out a null bit indicator only!
            if (fieldIsNull(nullIndicator, i))
                break;

            ColumnOffset endPointer;
            memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));
            // If we skipped to a column, the previous column offset has the beginning of our record.
            if (i > 0)
                memcpy(&rec_offset, directory_base + (i - 1) * sizeof(ColumnOffset), sizeof(ColumnOffset));

            // rec_offset keeps track of start of column, so end-start = total size
            uint32_t fieldSize = endPointer - rec_offset;

            // Special case for varchar, we must give data the size of varchar first
            if (recordDescriptor[i].type == TypeVarChar)
            {
                memcpy((char *)data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
                data_offset += VARCHAR_LENGTH_SIZE;
            }
            // Next we copy bytes equal to the size of the field and increase our offsets
            memcpy((char *)data + data_offset, start + rec_offset, fieldSize);
            break;
        }
    }

    free(pageData);
    return SUCCESS;
}

// Scan returns an iterator to allow the caller to go through the results one by one.
RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp,
                                const void *value, const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator)
{
    rbfm_ScanIterator.Open(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
    return SUCCESS;
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void *page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy(&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void *page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy(page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void *page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy(
        &recordEntry,
        ((char *)page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
        sizeof(SlotDirectoryRecordEntry));

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void *page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy(
        ((char *)page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
        &recordEntry,
        sizeof(SlotDirectoryRecordEntry));
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void *page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
    memcpy(page, &slotHeader, sizeof(SlotDirectoryHeader));
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char *)data, nullIndicatorSize);

    // Offset into *data. Start just after the null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize it to the size of the header
    unsigned size = sizeof(RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned)recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
        case TypeInt:
            size += INT_SIZE;
            offset += INT_SIZE;
            break;
        case TypeReal:
            size += REAL_SIZE;
            offset += REAL_SIZE;
            break;
        case TypeVarChar:
            uint32_t varcharSize;
            // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
            memcpy(&varcharSize, (char *)data + offset, VARCHAR_LENGTH_SIZE);
            size += varcharSize;
            offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for null-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount)
{
    return int(ceil((double)fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void *page)
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char *)page + offset;

    // Allocate space for null indicator.
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy(&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy(nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i + 1) / CHAR_BIT;
        int indicatorMask = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;

    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;

        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char *)data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char *)data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char *)data, nullIndicatorSize);

    // Points to start of record
    char *start = (char *)page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char *)data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
            case TypeInt:
                memcpy(start + rec_offset, data_start, INT_SIZE);
                rec_offset += INT_SIZE;
                data_offset += INT_SIZE;
                break;
            case TypeReal:
                memcpy(start + rec_offset, data_start, REAL_SIZE);
                rec_offset += REAL_SIZE;
                data_offset += REAL_SIZE;
                break;
            case TypeVarChar:
                unsigned varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                // We also have to account for the overhead given by that integer.
                rec_offset += varcharSize;
                data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.scan(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

RBFM_ScanIterator::RBFM_ScanIterator()
{
}

RBFM_ScanIterator::~RBFM_ScanIterator()
{
}

void RBFM_ScanIterator::Open(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp,
                             const void *value, const vector<string> &attributeNames)
{
    this->fileHandle = &fileHandle;
    this->totalPages = fileHandle.getNumberOfPages();
    this->recordDescriptor = &recordDescriptor;
    this->conditionAttribute = &conditionAttribute;
    this->compOp = compOp;
    this->value = value;
    this->attributeNames = &attributeNames;
}

// Never keep the results in the memory. When getNextRecord() is called,
// a satisfying record needs to be fetched from the file.
// "data" follows the same format as RecordBasedFileManager::insertRecord().
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
    while (this->pageNum < this->totalPages)
    {
        if (this->recordNum == 0)
        {
            if (this->fileHandle->readPage(this->pageNum, this->pageData))
                return RBFM_READ_FAILED;

            SlotDirectoryHeader header = rbfm->getSlotDirectoryHeader(this->pageData);
            this->totalRecordEntries = header.recordEntriesNumber;
            this->recordNum = 0;
        }

        SlotDirectoryRecordEntry recordEntry;
        while (this->recordNum < this->totalRecordEntries)
        {
            recordEntry = rbfm->getSlotDirectoryRecordEntry(this->pageData, this->recordNum);
            if (recordEntry.length < 0)
            {
                this->recordNum++;
                continue;
            }

            if (recordEntry.offset < 0)
            {
                this->recordNum++;
                continue;
            }

            if (!acceptRecord(recordEntry.offset))
            {
                this->recordNum++;
                continue;
            }

            // Pointer to start of record
            char *start = (char *)this->pageData + recordEntry.offset;

            // Allocate space for null indicator.
            int newNullIndicatorSize = rbfm->getNullIndicatorSize(this->attributeNames->size());
            char newNullIndicator[newNullIndicatorSize];
            memset(newNullIndicator, 0, newNullIndicatorSize);

            // Allocate space for null indicator.
            int nullIndicatorSize = rbfm->getNullIndicatorSize(this->recordDescriptor->size());
            char nullIndicator[nullIndicatorSize];
            memset(nullIndicator, 0, nullIndicatorSize);

            // Get number of columns and size of the null indicator for this record
            RecordLength len = 0;
            memcpy(&len, start, sizeof(RecordLength));
            int recordNullIndicatorSize = rbfm->getNullIndicatorSize(len);
            // Read in the existing null indicator
            memcpy(nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

            // Initialize some offsets
            // rec_offset: points to data in the record. We move this forward as we read data from our record
            unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
            // data_offset: points to our current place in the output data. We move this forward as we write to data.
            unsigned data_offset = nullIndicatorSize;
            // directory_base: points to the start of our directory of indices
            char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;

            unsigned newNullIndex = 0;
            for (unsigned i = 0; i < recordDescriptor->size(); i++)
            {
                if (rbfm->fieldIsNull(nullIndicator, i))
                {
                    if (std::find(attributeNames.begin(), attributeNames.end(), ))
                    {
                    }
                    // TODO: Set NULL indicator.
                    newNullIndex++;
                    continue;
                }

                // Grab pointer to end of this column
                ColumnOffset endPointer;
                memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

                // rec_offset keeps track of start of column, so end-start = total size
                uint32_t fieldSize = endPointer - rec_offset;

                // Special case for varchar, we must give data the size of varchar first
                if ((*recordDescriptor)[i].type == TypeVarChar)
                {
                    memcpy((char *)data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
                    data_offset += VARCHAR_LENGTH_SIZE;
                }
                // Next we copy bytes equal to the size of the field and increase our offsets
                memcpy((char *)data + data_offset, start + rec_offset, fieldSize);
                rec_offset += fieldSize;
                data_offset += fieldSize;
            }

            this->recordNum++;
        }

        // Increment the page number and reset the record num.
        this->pageNum++;
        this->recordNum = 0;
    }

    // TODO:
    // If it does, construct data to have the reduced number of null indicators.
    // Then, write in only the correct fields.
    // Use read record and read attribute as guides.

    return RBFM_EOF;
}

RC RBFM_ScanIterator::close()
{
    if (this->pageData != NULL)
        free(this->pageData);
    return SUCCESS;
}

bool RBFM_ScanIterator::acceptRecord(unsigned offset)
{
    // NoOp.
    if ((this->compOp == 6) || (this->value == NULL))
        return true;

    // Pointer to start of record
    char *start = (char *)this->pageData + offset;

    // Allocate space for null indicator.
    int nullIndicatorSize = rbfm->getNullIndicatorSize(this->recordDescriptor->size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy(&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = rbfm->getNullIndicatorSize(len);
    // Read in the existing null indicator
    memcpy(nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;

    unsigned i;
    for (i = 0; i < recordDescriptor->size(); i++)
    {
        if ((*recordDescriptor)[i].name == *conditionAttribute)
        {
            if (rbfm->fieldIsNull(nullIndicator, i))
                return false;

            ColumnOffset endPointer;
            memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));
            // If we skipped to a column, the previous column offset has the beginning of our record.
            if (i > 0)
                memcpy(&rec_offset, directory_base + (i - 1) * sizeof(ColumnOffset), sizeof(ColumnOffset));

            // rec_offset keeps track of start of column, so end-start = total size
            uint32_t fieldSize = endPointer - rec_offset;

            // Special case for varchar, we must give data the size of varchar first
            switch ((*recordDescriptor)[i].type)
            {
            case TypeInt:
                int data_integer;
                memcpy(&data_integer, ((char *)pageData + offset), INT_SIZE);
                offset += INT_SIZE;

                return intCompare(&data_integer);
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char *)pageData + offset), REAL_SIZE);
                offset += REAL_SIZE;

                return floatCompare(&data_real);
            case TypeVarChar:
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char *)pageData + data_offset), VARCHAR_LENGTH_SIZE);
                data_offset += VARCHAR_LENGTH_SIZE;
                // Gets the actual string.
                char *data_string = (char *)malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;

                memcpy(data_string, ((char *)pageData + data_offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                data_offset += varcharSize;

                bool ret_value = stringCompare(data_string, (*recordDescriptor)[i].length);
                free(data_string);
                return ret_value;
            }
        }
    }

    return false;
}

bool RBFM_ScanIterator::intCompare(int *compare)
{
    int val;
    memcpy(&val, value, INT_SIZE);
    switch (compOp)
    {
    case EQ_OP:
        return *compare == val;
    case LT_OP:
        return *compare < val;
    case LE_OP:
        return *compare <= val;
    case GT_OP:
        return *compare < val;
    case GE_OP:
        return *compare >= val;
    case NE_OP:
        return *compare != val;
    case NO_OP:
        return true;
    default:
        return false;
    }
}

bool RBFM_ScanIterator::floatCompare(float *compare)
{
    float val;
    memcpy(&val, value, sizeof(float));
    switch (compOp)
    {
    case EQ_OP:
        return *compare == val;
    case LT_OP:
        return *compare < val;
    case LE_OP:
        return *compare <= val;
    case GT_OP:
        return *compare < val;
    case GE_OP:
        return *compare >= val;
    case NE_OP:
        return *compare != val;
    case NO_OP:
        return true;
    default:
        return false;
    }
}

bool RBFM_ScanIterator::stringCompare(char *compare, uint32_t length)
{
    char str[length + 1];
    char *charValue = (char *)value;
    strcpy(str, charValue);
    int val = strcmp(compare, str);
    switch (compOp)
    {
    case EQ_OP:
        return val == 0;
    case LT_OP:
        return val < 0;
    case LE_OP:
        return val <= 0;
    case GT_OP:
        return val < 0;
    case GE_OP:
        return val >= 0;
    case NE_OP:
        return val != 0;
    case NO_OP:
        return true;
    default:
        return false;
    }
}

RC RBFM_ScanIterator::formatRecord(void *data, const vector<Attribute> &recordDescriptor, const vector<string> &attributeNames)
// Record to be formatted is expected to be in *data
{
    // Size of nullindicators and starting offset of field data
    unsigned outputNullIndicatorSize = rbfm->getNullIndicatorSize(attributeNames.size());
    unsigned inputNullIndicatorSize = rbfm->getNullIndicatorSize(recordDescriptor.size());

    char *tempBuffer = (char *)malloc(PAGE_SIZE); // Temp buffer to store formatted record
    memset(tempBuffer, 0, PAGE_SIZE);

    char *newNullIndicator = (char *)malloc(outputNullIndicatorSize); // NullIndicator for formatted record
    memset(newNullIndicator, 0, outputNullIndicatorSize);             // default 0 for nullIndicator

    char *dataPtr = (char *)data; // Char pointer to data
    unsigned formattedRecordOffset = outputNullIndicatorSize;
    unsigned recordOffset = inputNullIndicatorSize;

    // Process each attribute in attributeNames
    for (unsigned i = 0; i < attributeNames.size(); ++i)
    { // Loop for each requested attribute
        bool found = false;
        unsigned recordOffset = inputNullIndicatorSize;        // Resets the offset to the beginning of the record
        for (unsigned j = 0; j < recordDescriptor.size(); ++j) // Loop for each field in record
        {
            unsigned attributeSize = getAttributeSize(dataPtr + recordOffset, recordDescriptor[j]); // Returns the size of the attribute type

            if (recordDescriptor[j].name == attributeNames[i]) // Has to check if field name match since we don't know the ordering in attributeNames
            {
                if (rbfm->fieldIsNull(dataPtr, j)) // If null flag is found, set null in new nullIndicator
                {
                    setFieldNull(newNullIndicator, i);
                }
                else
                {
                    memcpy(tempBuffer + formattedRecordOffset, dataPtr + recordOffset, attributeSize); // Copys attribute data into formatted record
                    formattedRecordOffset += attributeSize;
                }
                found = true;
            }
            recordOffset += attributeSize; // Record offset is increased to stay consistent with loop
            if (found)                     // If attribute is found, we break to start looking for next attribute
                break;
        }
    }
    // Copy the new null indicator and formatted data back to the original data buffer
    memcpy(dataPtr, newNullIndicator, outputNullIndicatorSize);
    memcpy(dataPtr + outputNullIndicatorSize, tempBuffer + outputNullIndicatorSize, formattedRecordOffset - outputNullIndicatorSize);
    free(tempBuffer);
    free(newNullIndicator);
    return SUCCESS;
}

bool setFieldNull(char *nullIndicator, int fieldNum)
{
    int byteIndex = fieldNum / 8;                         // Finds the byte of the n'th field
    int bitPosition = fieldNum % 8;                       // Find the bit position within that byte
    nullIndicator[byteIndex] |= (1 << (7 - bitPosition)); // Set the bit to 1
}

unsigned getAttributeSize(const void *attributePtr, const Attribute &attribute)
{
    switch (attribute.type)
    {
    case TypeInt:
        return INT_SIZE; // Assuming int is 32-bit

    case TypeReal:
        return REAL_SIZE; // Typically 32-bit

    case TypeVarChar:
        // For VarChar, read the length from the data.
        // Using uint32_t for length ensures consistency across platforms.
        uint32_t length;
        memcpy(&length, attributePtr, VARCHAR_LENGTH_SIZE); // More specific than unsigned
        return VARCHAR_LENGTH_SIZE + length;                // Length of the string plus size of the length field itself

    default:
        // Ideally handle unknown types or add more types as needed
        return 0;
    }
}
