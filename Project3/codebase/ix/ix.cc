#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>

#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    if (fileExists(fileName))
        return IX_FILE_EXISTS;

    FILE *pFile = fopen(fileName.c_str(), "wb");
    if (pFile == NULL)
        return IX_CREATE_FAILED;
    fclose(pFile);

    IXFileHandle ixfileHandle;
    if (openFile(fileName, ixfileHandle) != SUCCESS) {
        return IX_CREATE_FAILED;
    }
    // cerr << "got here" << endl;
    // Setting up the header page.
    void * headerPageData = calloc(PAGE_SIZE, 1);
    if (headerPageData == NULL)
        return IX_MALLOC_FAILED;

    newHeaderPage(headerPageData);
    // Adds the header data page.
    if (ixfileHandle.appendPage(headerPageData))
        return IX_APPEND_FAILED;

    free(headerPageData);

    void * firstInternalPageData = calloc(PAGE_SIZE, 1);
    if (firstInternalPageData == NULL)
        return IX_MALLOC_FAILED;
    
    newInternalPage(firstInternalPageData, 2);
    if (ixfileHandle.appendPage(firstInternalPageData))
        return IX_APPEND_FAILED;

    free(firstInternalPageData);

    void * firstLeafPage = calloc(PAGE_SIZE, 1);
    if (firstLeafPage == NULL)
        return IX_MALLOC_FAILED;

    newLeafPage(firstLeafPage, 0, 0);
    if (ixfileHandle.appendPage(firstLeafPage))
        return IX_APPEND_FAILED;
    
    free(firstLeafPage);
    closeFile(ixfileHandle);
    return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
    // If file cannot be successfully removed, error
    if (remove(fileName.c_str()) != 0)
        return IX_REMOVE_FAILED;

    return SUCCESS;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    // If this handle already has an open file, error
    if (ixfileHandle.getfd() != NULL)
        return IX_HANDLE_IN_USE;

    // If the file doesn't exist, error
    if (!fileExists(fileName.c_str()))
        return IX_FILE_DN_EXIST;

    // Open the file for reading/writing in binary mode
    FILE *pFile;
    pFile = fopen(fileName.c_str(), "rb+");
    // If we fail, error
    if (pFile == NULL)
        return IX_OPEN_FAILED;

    ixfileHandle.setfd(pFile);

    return SUCCESS;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    FILE *pFile = ixfileHandle.getfd();

    // If not an open file, error
    if (pFile == NULL)
        return 1;

    // Flush and close the file
    fclose(pFile);

    ixfileHandle.setfd(NULL);

    return SUCCESS;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{  
    if (!isValidAttribute(attribute)){
        return IX_NO_SUCH_ATTR;
    }
    // First create data entry to be inserted
    unsigned rootPageNum = getRootPage(ixfileHandle); 
    // Will recursively walk down tree until it finds leaf page to insert
    SplitDataEntry splitEntry = insert(rootPageNum, attribute, key, rid, ixfileHandle);
    if (splitEntry.rc != SUCCESS)
        return splitEntry.rc;

    if (!splitEntry.isNull) {
        // We have a split at the root, so we can set left child page num to the old root.
        // We will insert the new data entry as the first entry in our new root page.
        void *pageData = calloc(PAGE_SIZE, 1);
        if (pageData == NULL)
            return IX_MALLOC_FAILED;

        // Create the new root page with LCP being the old root page num.
        unsigned newRootPageNum = ixfileHandle.getNumberOfPages();
        newInternalPage(pageData, rootPageNum);
        ixfileHandle.appendPage(pageData);
        // Insert the entry to the page that split from the root.
        splitEntry.rc = insertInInternal(pageData, newRootPageNum, attribute, splitEntry.data, splitEntry.dataEntry.rid, ixfileHandle);
        if (splitEntry.rc != SUCCESS)
            return IX_ROOT_SPLIT_FAILED;

        // Set the MetaDataHeader to point to the new root page.
        MetaDataHeader metaHeader;
        metaHeader.rootPageNum = newRootPageNum;
        memset(pageData, 0, PAGE_SIZE);
        setMetaDataHeader(pageData, metaHeader);
        ixfileHandle.writePage(0, pageData);
    }

    return SUCCESS;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{   
    if (!isValidAttribute(attribute)){
        return IX_NO_SUCH_ATTR;
    }

    int pageNum = findOptimalPage(attribute, key, ixfileHandle);
    void *pageData = malloc(PAGE_SIZE);
    if (ixfileHandle.readPage(pageNum, pageData) != SUCCESS) {
        free(pageData);
        return IX_READ_FAILED;
    }

    bool found = false;
    IndexHeader header = getIndexHeader(pageData);
    for (uint32_t i = 0; i < header.dataEntryNumber; i++) {
        IndexDataEntry dataEntry = getIndexDataEntry(pageData, i);
        RC value = compareKey(pageData, key, attribute, dataEntry);
        if (value == 0){ // We found data entry!
            RC res = deleteInLeaf(pageData, pageNum, attribute, i, ixfileHandle);
            if (res != SUCCESS) {
                free(pageData);
                return res;
            }
            found = true;
            header = getIndexHeader(pageData);// Refreshes header
            i--; // Decrements i to adjust for shift in entries
        }
    }
    free(pageData);
    if (!found) {
        return IX_REMOVE_FAILED; // Couldn't find data entry to delete
    }
    return SUCCESS; // 
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

/*
void spaces(int space) {
    int counter = 0;
    while(counter < space) {
        fprintf(stderr, " ");
        fflush(stderr);
        counter += 1;
    }
}

void printTree(Node *t, int depth) {
    if (t) {
        printTree(t->left, depth + 1);
        spaces(4 * depth);
        if (t->symbol != '$') {
            if (isgraph(t->symbol)) {
                fprintf(stderr, "'%c' (%" PRIu64 ")\n", t->symbol, t->frequency);
            } else {
                fprintf(stderr, "0x%02X (%" PRIu64 ")\n", t->symbol, t->frequency);
            }
        } else {
            fprintf(stderr, "$ (%" PRIu64 ")\n", t->frequency);
        }
        printTree(t->right, depth + 1);
    }
    return;
}
*/









void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    unsigned rootPageNum = printGetRootPage(ixfileHandle);
    // void *pageData = malloc(PAGE_SIZE);
    // if (fileHandle.readPage(rootPageNum, pageData) != SUCCESS){ // Assumption that the meta page is on page 0 of the file
    //     free(pageData);
    //     return IX_READ_FAILED;
    // }
    // cout << "{" << endl;
    switch(attribute.type) {
        case TypeInt:
            printTreeHelperInt(rootPageNum, 0, ixfileHandle);
            break;
        case TypeReal:
            printTreeHelperReal(rootPageNum, 0, ixfileHandle);
            break;
        case TypeVarChar:
            printTreeHelperVarChar(rootPageNum, 0, ixfileHandle);
            break;
    }
    cout << endl;
    // printTreeHelper(rootPageNum, 0, ixfileHandle, attribute)

    // cout << "}"<< endl;
    // free(pageData);
}

void IndexManager::printTreeHelperInt(uint32_t pageNum, uint16_t level, IXFileHandle &ixfileHandle) const {
    // to do add spacing based on level
    char* spaces = (char*)calloc(level*4 + 1, sizeof(char));
    for (int i = 0; i < level*4; i += 1) {
        spaces[i] = ' ';
    }
    spaces[level*4] = '\0';
    void *pageData = malloc(PAGE_SIZE);
    if (ixfileHandle.printReadPage(pageNum, pageData) != SUCCESS){ // Assumption that the meta page is on page 0 of the file
        free(pageData);
        cerr << "printing tree read page failed." << endl;
        return;
    }
    IndexHeader header = printGetIndexHeader(pageData);
    if (header.leaf) {
        // then no children
        cout << spaces << "{";
        cout << "\"keys\": [";

        for (uint32_t i = 0; i < header.dataEntryNumber; i += 1) {
            if (i != 0)
                cout << ",";
            IndexDataEntry curEntry = printGetIndexDataEntry(pageData, i);
            cout << curEntry.key << ": [(" << curEntry.rid.pageNum << "," << curEntry.rid.slotNum << ")]";
        }
        cout << "}";


    } 
    else {
        // with children
        uint32_t* children_pages = (uint32_t*)calloc(header.dataEntryNumber + 1, sizeof(uint32_t));
        children_pages[0] = header.leftChildPageNum;
        cout << spaces << "{" << endl;
        cout << spaces << "\"keys\": [";

        for (uint32_t i = 0; i < header.dataEntryNumber; i += 1) {
            if (i != 0)
                cout << ",";
            IndexDataEntry curEntry = printGetIndexDataEntry(pageData, i);
            children_pages[i+1] = curEntry.rid.pageNum;
            cout << curEntry.key;
        }
        cout << "]," << endl;
        cout << spaces << "\"children\": [" << endl;
        for (uint32_t i = 0; i < header.dataEntryNumber + 1; i += 1) {
            if (i != 0)
                cout << ",";
            printTreeHelperInt(children_pages[i], level + 1, ixfileHandle);
            cout << endl;
        }
        cout << spaces << "]}";
    }
    
    free(spaces);
    free(pageData);
}

void IndexManager::printTreeHelperReal(uint32_t pageNum, uint16_t level, IXFileHandle &ixfileHandle) const {
    // to do add spacing based on level
    char* spaces = (char*)calloc(level*4 + 1, sizeof(char));
    for (int i = 0; i < level*4; i += 1) {
        spaces[i] = ' ';
    }
    spaces[level*4] = '\0';
    void *pageData = malloc(PAGE_SIZE);
    if (ixfileHandle.printReadPage(pageNum, pageData) != SUCCESS){ // Assumption that the meta page is on page 0 of the file
        free(pageData);
        cerr << "printing tree read page failed." << endl;
        return;
    }
    IndexHeader header = printGetIndexHeader(pageData);
    if (header.leaf) {
        // then no children
        cout << spaces << "{";
        cout << "\"keys\": [";

        for (uint32_t i = 0; i < header.dataEntryNumber; i += 1) {
            if (i != 0)
                cout << ",";
            IndexDataEntry curEntry = printGetIndexDataEntry(pageData, i);
            cout << curEntry.key << ": [(" << curEntry.rid.pageNum << "," << curEntry.rid.slotNum << ")]";
        }
        cout << "}";


    } 
    else {
        // with children
        uint32_t* children_pages = (uint32_t*)calloc(header.dataEntryNumber + 1, sizeof(uint32_t));
        children_pages[0] = header.leftChildPageNum;
        cout << spaces << "{" << endl;
        cout << spaces << "\"keys\": [";

        for (uint32_t i = 0; i < header.dataEntryNumber; i += 1) {
            if (i != 0)
                cout << ",";
            IndexDataEntry curEntry = printGetIndexDataEntry(pageData, i);
            children_pages[i+1] = curEntry.rid.pageNum;
            cout << curEntry.key;
        }
        cout << "]," << endl;
        cout << spaces << "\"children\": [" << endl;
        for (uint32_t i = 0; i < header.dataEntryNumber + 1; i += 1) {
            if (i != 0)
                cout << ",";
            printTreeHelperReal(children_pages[i], level + 1, ixfileHandle);
            cout << endl;
        }
        cout << spaces << "]}";
    }
    
    free(spaces);
    free(pageData);
}
void IndexManager::printTreeHelperVarChar(uint32_t pageNum, uint16_t level, IXFileHandle &ixfileHandle) const {
    // to do add spacing based on level
    char* spaces = (char*)calloc(level*4 + 1, sizeof(char));
    for (int i = 0; i < level*4; i += 1) {
        spaces[i] = ' ';
    }
    spaces[level*4] = '\0';



    void *pageData = malloc(PAGE_SIZE);
    if (ixfileHandle.printReadPage(pageNum, pageData) != SUCCESS){ // Assumption that the meta page is on page 0 of the file
        free(pageData);
        cerr << "printing tree read page failed." << endl;
        return;
    }

    IndexHeader header = printGetIndexHeader(pageData);
    if (header.leaf) {
        // then no children
        cout << spaces << "{";
        cout << "\"keys\": [";

        for (uint32_t i = 0; i < header.dataEntryNumber; i += 1) {
            if (i != 0)
                cout << ",";
            IndexDataEntry curEntry = printGetIndexDataEntry(pageData, i);
            uint32_t length;
            memcpy(&length, (char*)pageData + curEntry.key, VARCHAR_LENGTH_SIZE);

            char* buf = (char*)malloc(length + 1);
            memcpy(buf, (char*)pageData + curEntry.key + VARCHAR_LENGTH_SIZE, length);
            buf[length] = '\0';
            cout << buf << ": [(" << curEntry.rid.pageNum << "," << curEntry.rid.slotNum << ")]";
            free(buf);
        }
        cout << "}";


    } 
    else {
        // with children
        uint32_t* children_pages = (uint32_t*)calloc(header.dataEntryNumber + 1, sizeof(uint32_t));
        children_pages[0] = header.leftChildPageNum;
        cout << spaces << "{" << endl;
        cout << spaces << "\"keys\": [";

        for (uint32_t i = 0; i < header.dataEntryNumber; i += 1) {
            if (i != 0)
                cout << ",";
            IndexDataEntry curEntry = printGetIndexDataEntry(pageData, i);

            uint32_t length;
            memcpy(&length, (char*)pageData + curEntry.key, VARCHAR_LENGTH_SIZE);

            char* buf = (char*)malloc(length + 1);
            memcpy(buf, (char*)pageData + curEntry.key + VARCHAR_LENGTH_SIZE, length);
            buf[length] = '\0';

            children_pages[i+1] = curEntry.rid.pageNum;

            cout << buf;
            free(buf);
        }
        cout << "]," << endl;
        cout << spaces << "\"children\": [" << endl;
        for (uint32_t i = 0; i < header.dataEntryNumber + 1; i += 1) {
            if (i != 0)
                cout << "," << endl;
            printTreeHelperReal(children_pages[i], level + 1, ixfileHandle);
            cout << endl;
        }
        cout << spaces << "]}";
    }
    
    free(spaces);
    free(pageData);
}

//need to create const versions of a lot of reading functions
unsigned IndexManager::printGetRootPage(const IXFileHandle &fileHandle) const {
    void *pageData = malloc(PAGE_SIZE);
    if (fileHandle.printReadPage(0, pageData) != SUCCESS){ // Assumption that the meta page is on page 0 of the file
        free(pageData);
        return IX_READ_FAILED;
    }

    MetaDataHeader metaHeader = printGetMetaDataHeader(pageData);
    unsigned rootPageNum = metaHeader.rootPageNum;
    free(pageData);
    return rootPageNum;
}

IndexHeader IndexManager::printGetIndexHeader(void *pageData) const {
    IndexHeader indexHeader;
    memcpy(&indexHeader, pageData, sizeof(IndexHeader));
    return indexHeader;
}

IndexDataEntry IndexManager::printGetIndexDataEntry(const void *pageData, const unsigned indexEntryNumber) const {
    IndexDataEntry dataEntry;
    memcpy(
            &dataEntry,
            ((char *) pageData + sizeof(IndexHeader) + (indexEntryNumber * sizeof(IndexDataEntry))),
            sizeof(IndexDataEntry)
          );
    return dataEntry;
}

MetaDataHeader IndexManager::printGetMetaDataHeader(const void *pageData) const {
    // Getting the metadata header.
    MetaDataHeader metaHeader;
    memcpy(&metaHeader, pageData, sizeof(MetaDataHeader));
    return metaHeader;
}

RC IXFileHandle::printReadPage(PageNum pageNum, void *data) const {
    // If pageNum doesn't exist, error
    if (printGetNumberOfPages() < pageNum)
        return FH_PAGE_DN_EXIST;

    // Try to seek to the specified page
    if (fseek(_fd, PAGE_SIZE * pageNum, SEEK_SET))
        return FH_SEEK_FAILED;

    // Try to read the specified page
    if (fread(data, 1, PAGE_SIZE, _fd) != PAGE_SIZE)
        return FH_READ_FAILED;
    return SUCCESS;
}

unsigned IXFileHandle::printGetNumberOfPages() const {
    // Use stat to get the file size
    struct stat sb;
    if (fstat(fileno(_fd), &sb) != 0)
        // On error, return 0
        return 0;
    // Filesize is always PAGE_SIZE * number of pages
    return sb.st_size / PAGE_SIZE;
}















IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return SUCCESS;
}

void IndexManager::newHeaderPage(void *pageData)
{
    memset(pageData, 0, PAGE_SIZE);
    // Write the header page struct.
    MetaDataHeader metaHeader;
    metaHeader.rootPageNum = 1;
    setMetaDataHeader(pageData, metaHeader);
}

void IndexManager::setMetaDataHeader(void *pageData, MetaDataHeader metaHeader)
{
    // Setting the metadata header.
    memcpy(pageData, &metaHeader, sizeof(MetaDataHeader));
}

MetaDataHeader IndexManager::getMetaDataHeader(void *pageData)
{
    // Getting the metadata header.
    MetaDataHeader metaHeader;
    memcpy(&metaHeader, pageData, sizeof(MetaDataHeader));
    return metaHeader;
}

void IndexManager::newInternalPage(void *pageData, int leftChildPageNum)
{
    memset(pageData, 0, PAGE_SIZE);
    // Write internal page header.
    IndexHeader internalHeader;
    internalHeader.leaf = false;
    internalHeader.dataEntryNumber = 0;
    internalHeader.freeSpaceOffset = PAGE_SIZE;
    internalHeader.leftChildPageNum = leftChildPageNum;
    // Initialize values, even if they aren't needed.
    // Page num 0 will always be the meta data page, so this is an invalid internal or leaf page.
    internalHeader.nextSiblingPageNum = 0;
    internalHeader.prevSiblingPageNum = 0;
    setIndexHeader(pageData, internalHeader);
}

void IndexManager::newLeafPage(void *pageData, int nextSiblingPageNum, int prevSiblingPageNum)
{
    memset(pageData, 0, PAGE_SIZE);
    // Write Leaf Header struct.
    IndexHeader newHeader;
    newHeader.leaf = true;
    newHeader.dataEntryNumber = 0;
    newHeader.nextSiblingPageNum = nextSiblingPageNum;
    newHeader.prevSiblingPageNum = prevSiblingPageNum;
    newHeader.freeSpaceOffset = PAGE_SIZE;
    // Initialize values, even if they aren't needed.
    // Page num 0 will always be the meta data page, so this is an invalid internal or leaf page.
    newHeader.leftChildPageNum = 0;
    setIndexHeader(pageData, newHeader);
}

void IndexManager::setIndexHeader(void *pageData, IndexHeader indexHeader)
{
    // Set the index header.
    memcpy((char *)pageData, &indexHeader, sizeof(IndexHeader));
}

IndexHeader IndexManager::getIndexHeader(void *pageData)
{
    // Get Index Header.
    IndexHeader indexHeader;
    memcpy(&indexHeader, pageData, sizeof(IndexHeader));
    return indexHeader;
}

void IndexManager::setIndexDataEntry(void *pageData, unsigned indexEntryNumber, IndexDataEntry dataEntry)
{
    // Setting the index data entry.
    memcpy(
            ((char *)pageData + sizeof(IndexHeader) + (indexEntryNumber * sizeof(IndexDataEntry))),
            &dataEntry,
            sizeof(IndexDataEntry)
          );
}

IndexDataEntry IndexManager::getIndexDataEntry(void *pageData, unsigned indexEntryNumber)
{
    IndexDataEntry dataEntry;
    memcpy(
            &dataEntry,
            ((char *) pageData + sizeof(IndexHeader) + (indexEntryNumber * sizeof(IndexDataEntry))),
            sizeof(IndexDataEntry)
          );
    return dataEntry;
}

unsigned IndexManager::getPageFreeSpaceSize(void *pageData)
{
    IndexHeader indexHeader = getIndexHeader(pageData);
    return indexHeader.freeSpaceOffset - (indexHeader.dataEntryNumber * sizeof(IndexDataEntry)) - sizeof(IndexHeader);
}

bool IndexManager::canFitEntry(void *pageData, const Attribute &attr, const void *key)
{
    unsigned freeSpaceSize = getPageFreeSpaceSize(pageData);
    unsigned spaceNeeded = sizeof(IndexDataEntry);
    if (attr.type == TypeVarChar) {
        int length;
        memcpy(&length, key, sizeof(int));
        spaceNeeded += sizeof(int) + length;
    }

    return spaceNeeded <= freeSpaceSize;
}

SplitDataEntry IndexManager::insert(unsigned pageNum, const Attribute &attr, const void *key, const RID &rid, IXFileHandle &fileHandle)
{
    SplitDataEntry splitEntry;
    void *pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(pageNum, pageData) != SUCCESS){ // Assuming that IXFileHandle has identical methods as other FileHandle class
        free(pageData);
        splitEntry.rc = IX_READ_FAILED;
        splitEntry.isNull = true;
        return splitEntry;
    }
    
    // Checks if current page is a leaf or internal page
    if (isNonLeaf(pageData)) {
        // If internal page, get child page, and recursively call insert.
        unsigned childPageNum = getChildPageNum(pageData, key, attr);
        // getChildPageNum returns 0 on error.
        if (childPageNum == 0) {
            splitEntry.rc = IX_EXISTING_ENTRY; 
            splitEntry.isNull = true;
            return splitEntry;
        }
        
        splitEntry = insert(childPageNum, attr, key, rid, fileHandle); 
        if (splitEntry.rc != SUCCESS)
            return splitEntry;

        if (!splitEntry.isNull) {
            // This will be called at every backtrack level until we find space to insert.
            splitEntry.rc = insertInInternal(pageData, pageNum, attr, key, rid, fileHandle);
            if ((splitEntry.rc != SUCCESS) && (splitEntry.rc != IX_INTERNAL_SPLIT)) {
                splitEntry.isNull = true;
                return splitEntry;
            }

            if (splitEntry.rc == IX_INTERNAL_SPLIT){
                splitEntry = splitInternal(pageData, pageNum, attr, key, rid, fileHandle);
                return splitEntry;
            }
        }
    } else {
        // If leaf page, attempt to insert data entry in leaf
        splitEntry.rc = insertInLeaf(pageData, pageNum, attr, key, rid, fileHandle);
        if ((splitEntry.rc != SUCCESS) && (splitEntry.rc != IX_LEAF_SPLIT)) {
            splitEntry.isNull = true;
            return splitEntry;
        }

        if (splitEntry.rc == IX_LEAF_SPLIT) {
            splitEntry = splitLeaf(pageData, pageNum, attr, key, rid, fileHandle);
            return splitEntry;
        }

        fileHandle.writePage(pageNum, pageData);
    }

    free(pageData);
    splitEntry.isNull = true;
    splitEntry.rc = SUCCESS;
    return splitEntry;
}

unsigned IndexManager::getRootPage(IXFileHandle &fileHandle)
{
    void *pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(0, pageData) != SUCCESS){ // Assumption that the meta page is on page 0 of the file
        free(pageData);
        return IX_READ_FAILED;
    }

    MetaDataHeader metaHeader = getMetaDataHeader(pageData);
    unsigned rootPageNum = metaHeader.rootPageNum;
    free(pageData);
    return rootPageNum;
}

bool IndexManager::isNonLeaf(void *pageData)
{
    IndexHeader header = getIndexHeader(pageData);
    return !header.leaf; // Will return true of it is a non-leaf page
}

unsigned IndexManager::getChildPageNum(void *pageData, const void *key, const Attribute &attr)
{
    IndexHeader header = getIndexHeader(pageData);
    /* unsigned offset = sizeof(IndexHeader); */
    uint32_t lastChildPage = header.leftChildPageNum;

    for (unsigned i = 0; i < header.dataEntryNumber; i++) {
        // unsigned entryOffset = offset + i * sizeof(IndexDataEntry); // Calculates current iteration's entry offset

        // Accesses current entry to grab update child page later
        IndexDataEntry entry = getIndexDataEntry(pageData, i);
        // memcpy(&entry, (char*)pageData + entryOffset, sizeof(IndexDataEntry));

        int result = compareKey(pageData, key, attr, entry);
        if (result == -1) // If the provided key is less than the current entry's key, return the last child page number encountered
            return lastChildPage;

        // compareKey returns 1 if we must keep searching.
        if (result != 1)
            return 0;

        lastChildPage = entry.rid.pageNum;
    }

    // If the key is greater than all the keys in the entries, return the last child page number
    return lastChildPage;
}

RC IndexManager::insertInInternal(void *pageData, unsigned pageNum, const Attribute &attr, const void *key, const RID &rid, IXFileHandle &fileHandle)
{
    RC rc = insertInLeaf(pageData, pageNum, attr, key, rid, fileHandle);
    if (rc == IX_LEAF_SPLIT)
        return IX_INTERNAL_SPLIT;

    return rc;
}

SplitDataEntry IndexManager::splitInternal(void *pageData, unsigned pageNum, const Attribute &attr, const void *key,
        const RID &rid, IXFileHandle &fileHandle)
{
    // Should attempt to place internal "traffic cop" within page. If successful, set key within data entry to null 
    bool varchar = false;
    if (attr.type == TypeVarChar)
        varchar = true;

    IndexHeader indexHeader = getIndexHeader(pageData);
    // Current page will keep half of the entries.
    uint32_t numOldEntries = floor(indexHeader.dataEntryNumber / 2);
    // We skip one entry because it will be copied up.
    unsigned indexOfFirstEntry = numOldEntries + 1;
    // Get half of the entries for the new page.
    uint32_t numNewEntries = ceil(indexHeader.dataEntryNumber / 2) - 1;

    SplitDataEntry splitEntry;
    splitEntry.dataEntry = getIndexDataEntry(pageData, numOldEntries);
    splitEntry.isTypeVarChar = varchar;
    splitEntry.rc = SUCCESS;
    splitEntry.isNull = false;
    if (varchar) {
        int length;
        memcpy(&length, (char *)pageData + splitEntry.dataEntry.key, sizeof(int));
        int totalLength = sizeof(int) + length;
        splitEntry.data = calloc(totalLength, 1);
        memcpy((char *)splitEntry.data, (char *)pageData + splitEntry.dataEntry.key, totalLength);
    } else {
        splitEntry.data = NULL;
    }

    void *newPageData = calloc(PAGE_SIZE, 1);
    if (newPageData == NULL) {
        splitEntry.rc = IX_MALLOC_FAILED;
        return splitEntry;
    }
    
    // Must compare the new key with the first key on the new page.
    // Then, we can call insert on the split leaf page because we must have room.
    IndexDataEntry firstEntry = getIndexDataEntry(pageData, indexOfFirstEntry);
    RC value = compareKey(pageData, key, attr, firstEntry);
    bool newPageInsert;
    if (value == -1) {
        newPageInsert = false;
    } else {
        newPageInsert = true;
    }

    // Update the old index header.
    indexHeader.dataEntryNumber = numOldEntries;
    indexHeader.nextSiblingPageNum = fileHandle.getNumberOfPages();
    if (newPageInsert)
        insertInInternal(newPageData, pageNum, attr, key, rid, fileHandle);

    // Prepare the new pages header.
    IndexHeader newHeader;
    newHeader.leaf = true;
    newHeader.dataEntryNumber = numNewEntries;
    newHeader.prevSiblingPageNum = pageNum;
    newHeader.nextSiblingPageNum = indexHeader.nextSiblingPageNum;
    newHeader.leftChildPageNum = 0;
    newHeader.freeSpaceOffset = PAGE_SIZE;

    setIndexHeader(newPageData, newHeader);
    newPageFromEntries(pageData, newPageData, indexOfFirstEntry, numNewEntries, varchar);
    /* IndexDataEntry newDataEntry = getIndexDataEntry(pageData, 0); */

    fileHandle.appendPage(newPageData);

    // Update the prevSiblingPageNum of the old nextSiblingPageNum.
    if (newHeader.nextSiblingPageNum != 0) {
        memset(newPageData, 0, PAGE_SIZE);
        fileHandle.readPage(newHeader.nextSiblingPageNum, newPageData);

        IndexHeader siblingHeader = getIndexHeader(newPageData);
        siblingHeader.prevSiblingPageNum = indexHeader.nextSiblingPageNum;
        setIndexHeader(newPageData, siblingHeader);

        fileHandle.writePage(newHeader.nextSiblingPageNum, newPageData);
    }

    // Restructure the current page data by writing the information into the temp page.
    memset(newPageData, 0, PAGE_SIZE);
    setIndexHeader(newPageData, indexHeader);
    newPageFromEntries(pageData, newPageData, 0, numOldEntries, varchar);
    if (!newPageInsert)
        insertInInternal(newPageData, pageNum, attr, key, rid, fileHandle);

    fileHandle.writePage(pageNum, newPageData);

    free(newPageData);
    return splitEntry;
}

RC IndexManager::insertInLeaf(void *pageData, unsigned pageNum, const Attribute &attr, const void *key, const RID &rid, IXFileHandle &fileHandle)
{
    // Should return SUCCESS if new data entry is inserted into leaf page. If page is full, return IX_INSERT_SPLIT 
    if (!canFitEntry(pageData, attr, key))
        return IX_LEAF_SPLIT;

    IndexHeader header = getIndexHeader(pageData);

    // Prepare the new data entry and write var char if necessary.
    IndexDataEntry dataEntry;
    dataEntry.rid = rid;
    uint16_t offset = header.freeSpaceOffset;
    if (attr.type == TypeVarChar) {
        int length;
        memcpy(&length, key, sizeof(int));
        int totalLength = sizeof(int) + length;
        offset -= totalLength;
        memcpy((char *)pageData + offset, key, totalLength);
        header.freeSpaceOffset = offset;
        dataEntry.key = (int)offset;
    } else {
        memcpy(&(dataEntry.key), key, sizeof(int));
    }

    // Find the appropriate entry number for the new entry.
    uint32_t entryNum = 0;
    uint32_t i;
    for (i = 0; i < header.dataEntryNumber; i++) {
        // Iteratively compare key until we find it's slot.
        dataEntry = getIndexDataEntry(pageData, i);
        if (compareKey(pageData, key, attr, dataEntry) != -1)
            break;

        entryNum++;
    }

    // Shift over data entries after the slot we want to insert.
    IndexDataEntry shiftDataEntry;
    for (i = header.dataEntryNumber; i > entryNum; i--) {
       shiftDataEntry = getIndexDataEntry(pageData, i - 1);
       setIndexDataEntry(pageData, i, shiftDataEntry);
    }

    // Write the new data entry.
    setIndexDataEntry(pageData, entryNum, dataEntry);

    header.dataEntryNumber++;
    setIndexHeader(pageData, header);

    fileHandle.writePage(pageNum, pageData);
    return SUCCESS;
}

SplitDataEntry IndexManager::splitLeaf(void *pageData, unsigned pageNum, const Attribute &attr, const void *key,
        const RID &rid, IXFileHandle &fileHandle)
{
    bool varchar = false;
    if (attr.type == TypeVarChar)
        varchar = true;

    IndexHeader indexHeader = getIndexHeader(pageData);
    // Current page will keep half of the entries.
    uint32_t numOldEntries = floor(indexHeader.dataEntryNumber / 2);
    // We skip one entry because it will be copied up.
    unsigned indexOfFirstEntry = numOldEntries;
    // Get half of the entries for the new page.
    uint32_t numNewEntries = ceil(indexHeader.dataEntryNumber / 2);

    SplitDataEntry splitEntry;
    splitEntry.dataEntry = getIndexDataEntry(pageData, numOldEntries);
    splitEntry.isTypeVarChar = varchar;
    splitEntry.rc = SUCCESS;
    splitEntry.isNull = false;
    if (varchar) {
        int length;
        memcpy(&length, (char *)pageData + splitEntry.dataEntry.key, sizeof(int));
        int totalLength = sizeof(int) + length;
        splitEntry.data = calloc(totalLength, 1);
        memcpy((char *)splitEntry.data, (char *)pageData + splitEntry.dataEntry.key, totalLength);
    } else {
        splitEntry.data = NULL;
    }

    void *newPageData = calloc(PAGE_SIZE, 1);
    if (newPageData == NULL) {
       splitEntry.rc = IX_MALLOC_FAILED;
       return splitEntry;
    }
    
    // Must compare the new key with the first key on the new page.
    // Then, we can call insert on the split leaf page because we must have room.
    IndexDataEntry firstEntry = getIndexDataEntry(pageData, indexOfFirstEntry);
    RC value = compareKey(pageData, key, attr, firstEntry);
    bool newPageInsert;
    if (value == -1) {
        newPageInsert = false;
    } else {
        newPageInsert = true;
    }

    // Update the old index header.
    indexHeader.dataEntryNumber = numOldEntries;
    indexHeader.nextSiblingPageNum = fileHandle.getNumberOfPages();
    if (newPageInsert)
        insertInLeaf(newPageData, pageNum, attr, key, rid, fileHandle);

    // Prepare the new pages header.
    IndexHeader newHeader;
    newHeader.leaf = true;
    newHeader.dataEntryNumber = numNewEntries;
    newHeader.prevSiblingPageNum = pageNum;
    newHeader.nextSiblingPageNum = indexHeader.nextSiblingPageNum;
    newHeader.leftChildPageNum = 0;
    newHeader.freeSpaceOffset = PAGE_SIZE;

    setIndexHeader(newPageData, newHeader);
    newPageFromEntries(pageData, newPageData, indexOfFirstEntry, numNewEntries, varchar);
    /* IndexDataEntry newDataEntry = getIndexDataEntry(pageData, 0); */

    fileHandle.appendPage(newPageData);

    // Update the prevSiblingPageNum of the old nextSiblingPageNum.
    if (newHeader.nextSiblingPageNum != 0) {
        memset(newPageData, 0, PAGE_SIZE);
        fileHandle.readPage(newHeader.nextSiblingPageNum, newPageData);

        IndexHeader siblingHeader = getIndexHeader(newPageData);
        siblingHeader.prevSiblingPageNum = indexHeader.nextSiblingPageNum;
        setIndexHeader(newPageData, siblingHeader);

        fileHandle.writePage(newHeader.nextSiblingPageNum, newPageData);
    }

    // Restructure the current page data by writing the information into the temp page.
    memset(newPageData, 0, PAGE_SIZE);
    setIndexHeader(newPageData, indexHeader);
    newPageFromEntries(pageData, newPageData, 0, numOldEntries, varchar);
    if (!newPageInsert)
        insertInLeaf(newPageData, pageNum, attr, key, rid, fileHandle);

    fileHandle.writePage(pageNum, newPageData);

    free(newPageData);
    return splitEntry;
}

// returns page number of first leaf page to look at for possible value 
RC IndexManager::findOptimalPage(const Attribute &attr, const void* key, IXFileHandle &fileHandle) {
    unsigned rootpage = getRootPage(fileHandle);
    void* cur = calloc(PAGE_SIZE, 1);
    fileHandle.readPage(rootpage, cur);

    IndexHeader header = getIndexHeader(cur);
    if (header.dataEntryNumber == 0)
        return header.leftChildPageNum;
    // otherwise there are entries and we need to start checking them
    IndexDataEntry entry;
    
    switch (attr.type) {
    case TypeInt:
        int32_t key_val_int;
        memcpy(&key_val_int, key, 4);
        for (uint32_t i = 0; i < header.dataEntryNumber; i += 1) {
            entry = getIndexDataEntry(cur, i);
            if (key_val_int <= entry.key) {
                if (i == 0) {
                    return optimalPageHelper(attr, key, fileHandle, header.leftChildPageNum);
                }
                else {
                    IndexDataEntry correct_entry = getIndexDataEntry(cur, i - 1);
                    return optimalPageHelper(attr, key, fileHandle, correct_entry.rid.pageNum);
                }
                
            }
            // else if (key_val_int == entry.key) {
            //     return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);
            // }
        }
        return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);// break;
    case TypeReal:
        float key_val;
        memcpy(&key_val, key, 4);
        for (uint32_t i = 0; i < header.dataEntryNumber; i += 1) {
            entry = getIndexDataEntry(cur, i);
            if (key_val <= entry.key) {
                if (i == 0) {
                    return optimalPageHelper(attr, key, fileHandle, header.leftChildPageNum);
                }
                else {
                    IndexDataEntry correct_entry = getIndexDataEntry(cur, i - 1);
                    return optimalPageHelper(attr, key, fileHandle, correct_entry.rid.pageNum);
                }
                
            }
            // else if (key_val == entry.key) {
            //     return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);
            // }
        }
        return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);// break;
    case TypeVarChar:
        for (uint32_t i = 0; i < header.dataEntryNumber; i += 1) {
            entry = getIndexDataEntry(cur, i);
            //get length of current varchar data entry
            uint32_t length;
            memcpy(&length, (char*)cur + entry.key, VARCHAR_LENGTH_SIZE);
            //read varchar into buffer
            char* buf = (char*)malloc(length + 1);
            memcpy(buf, (char*)cur + entry.key + VARCHAR_LENGTH_SIZE, length);
            buf[length] = '\0';

            //get their varchar key and null terminate it
            uint32_t key_length;
            memcpy(&key_length, key, VARCHAR_LENGTH_SIZE);
            char* key_buf = (char*)malloc(key_length + 1);
            memcpy(key_buf, key + VARCHAR_LENGTH_SIZE, key_length);
            key_buf[key_length] = '\0';


            int cmp = strcmp(key_buf, buf);
            if (cmp <= 0) {
                // key is less than what current index entry is, meaning we need to look at previous page
                if (i == 0) {
                    return optimalPageHelper(attr, key, fileHandle, header.leftChildPageNum);
                }
                else {
                    IndexDataEntry correct_entry = getIndexDataEntry(cur, i - 1);
                    return optimalPageHelper(attr, key, fileHandle, correct_entry.rid.pageNum);
                }
            }
            // if (cmp == 0) {
            //     return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);
            // }
        }
        // if we get here without having returned a value, then we return the final page entry thingy
        return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);// break;
    }
    return -101;   
}

RC IndexManager::optimalPageHelper(const Attribute &attr, const void* key, IXFileHandle &fileHandle, uint32_t pageNum) {
    void* cur = calloc(PAGE_SIZE, 1);
    fileHandle.readPage(pageNum, cur);
    IndexHeader header = getIndexHeader(cur);
    if (header.leaf)
        return pageNum;

    if (header.dataEntryNumber == 0)
        return header.leftChildPageNum;
    
    IndexDataEntry entry;
    switch (attr.type) {
    case TypeInt:
        int32_t key_val_int;
        memcpy(&key_val_int, key, 4);
        for (uint32_t i = 0; i < header.dataEntryNumber; i += 1) {
            entry = getIndexDataEntry(cur, i);
            if (key_val_int <= entry.key) {
                if (i == 0) {
                    return optimalPageHelper(attr, key, fileHandle, header.leftChildPageNum);
                }
                else {
                    IndexDataEntry correct_entry = getIndexDataEntry(cur, i - 1);
                    return optimalPageHelper(attr, key, fileHandle, correct_entry.rid.pageNum);
                }
                
            }
            // else if (key_val_int == entry.key) {
            //     return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);
            // }
        }
        return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);
        break;
    case TypeReal:
        float key_val;
        memcpy(&key_val, key, 4);
        for (uint32_t i = 0; i < header.dataEntryNumber; i += 1) {
            entry = getIndexDataEntry(cur, i);
            if (key_val <= entry.key) {
                if (i == 0) {
                    return optimalPageHelper(attr, key, fileHandle, header.leftChildPageNum);
                }
                else {
                    IndexDataEntry correct_entry = getIndexDataEntry(cur, i - 1);
                    return optimalPageHelper(attr, key, fileHandle, correct_entry.rid.pageNum);
                }
                
            }
            // else if (key_val == entry.key) {
            //     return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);
            // }
        }
        return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);// break;
    case TypeVarChar:
        for (uint32_t i = 0; i < header.dataEntryNumber; i += 1) {
            entry = getIndexDataEntry(cur, i);
            //get length of current varchar data entry
            uint32_t length;
            memcpy(&length, (char*)cur + entry.key, VARCHAR_LENGTH_SIZE);
            //read varchar into buffer
            char* buf = (char*)malloc(length + 1);
            memcpy(buf, (char*)cur + entry.key + VARCHAR_LENGTH_SIZE, length);
            buf[length] = '\0';

            //get their varchar key and null terminate it
            uint32_t key_length;
            memcpy(&key_length, key, VARCHAR_LENGTH_SIZE);
            char* key_buf = (char*)malloc(key_length + 1);
            memcpy(key_buf, key + VARCHAR_LENGTH_SIZE, key_length);
            key_buf[key_length] = '\0';


            int cmp = strcmp(key_buf, buf);
            if (cmp <= 0) {
                // key is less than what current index entry is, meaning we need to look at previous page
                if (i == 0) {
                    return optimalPageHelper(attr, key, fileHandle, header.leftChildPageNum);
                }
                else {
                    IndexDataEntry correct_entry = getIndexDataEntry(cur, i - 1);
                    return optimalPageHelper(attr, key, fileHandle, correct_entry.rid.pageNum);
                }
            }
            // if (cmp == 0) {
            //     return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);
            // }
        }
        // if we get here without having returned a value, then we return the final page entry thingy
        return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);// break;
    }
    return -99;
}

    // if (attr.type == TypeVarChar) {
    //     for (uint32_t i = 0; i < header.dataEntryNumber; i += 1) {
    //         entry = getIndexDataEntry(cur, i);
    //         //get length of current varchar data entry
    //         uint32_t length;
    //         memcpy(&length, (char*)cur + entry.key, VARCHAR_LENGTH_SIZE);
    //         //read varchar into buffer
    //         char* buf = (char*)malloc(length + 1);
    //         memcpy(buf, (char*)cur + entry.key + VARCHAR_LENGTH_SIZE, length);
    //         buf[length] = '\0';

    //         //get their varchar key and null terminate it
    //         uint32_t key_length;
    //         memcpy(&key_length, key, VARCHAR_LENGTH_SIZE);
    //         char* key_buf = (char*)malloc(key_length + 1);
    //         memcpy(key_buf, key + VARCHAR_LENGTH_SIZE, key_length);
    //         key_buf[key_length] = '\0';


    //         int cmp = strcmp(key_buf, buf);
    //         if (cmp < 0) {
    //             // key is less than what current index entry is, meaning we need to look at previous page
    //             if (i == 0) {
    //                 return optimalPageHelper(attr, key, fileHandle, header.leftChildPageNum);
    //             }
    //             else {
    //                 IndexDataEntry correct_entry = getIndexDataEntry(cur, i - 1);
    //                 return optimalPageHelper(attr, key, fileHandle, correct_entry.rid.pageNum);
    //             }
    //         }
    //         if (cmp == 0) {
    //             return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);
    //         }
    //     }
    //     // if we get here without having returned a value, then we return the final page entry thingy
    //     return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);
    // }
    // else {
    //     // NEED TO CREATE FLOAT CASE

    //     int32_t key_val;
    //     memcpy(&key_val, key, 4);
    //     for (uint32_t i = 0; i < header.dataEntryNumber; i += 1) {
    //         entry = getIndexDataEntry(cur, i);
    //         if (key_val < entry.key) {
    //             if (i == 0) {
    //                 return optimalPageHelper(attr, key, fileHandle, header.leftChildPageNum);
    //             }
    //             else {
    //                 IndexDataEntry correct_entry = getIndexDataEntry(cur, i - 1);
    //                 return optimalPageHelper(attr, key, fileHandle, correct_entry.rid.pageNum);
    //             }
                
    //         }
    //         else if (key_val == entry.key) {
    //             return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);
    //         }
    //     }
    //     return optimalPageHelper(attr, key, fileHandle, entry.rid.pageNum);
    // }
/* } */

// RC IndexManager::readVarCharDataEntry(IXFileHandle &fileHandle, const void* pageData, const IndexDataEntry &dataEntry) {
    
//     uint32_t length;
//     memcpy(&length, (char*)pageData + dataEntry.key, VARCHAR_LENGTH_SIZE);

//     char* buf = (char*)malloc(length + 1);
//     memcpy(buf, (char*)pageData + dataEntry.key + VARCHAR_LENGTH_SIZE, length);
//     buf[length] = '\0';
    
// }

RC IndexManager::compareKey(void* pageData, const void* key, const Attribute &attr, IndexDataEntry &entry) {
    switch(attr.type) {
        case TypeInt:
            int32_t key_val_int;
            memcpy(&key_val_int, key, 4);
            if (key_val_int < entry.key){
                return -1;
            }
            else if (key_val_int > entry.key) {
                return 1;
            }
            else {
                return 0;
            }
            break;
        case TypeReal:
            float key_val;
            memcpy(&key_val, key, 4);
            if (key_val < entry.key){
                return -1;
            }
            else if (key_val > entry.key) {
                return 1;
            }
            else {
                return 0;
            }
            break;
        case TypeVarChar:
            uint32_t length;
            memcpy(&length, (char*)pageData + entry.key, VARCHAR_LENGTH_SIZE);
            //read varchar into buffer
            char* buf = (char*)malloc(length + 1);
            memcpy(buf, (char*)pageData + entry.key + VARCHAR_LENGTH_SIZE, length);
            buf[length] = '\0';

            //get their varchar key and null terminate it
            uint32_t key_length;
            memcpy(&key_length, key, VARCHAR_LENGTH_SIZE);
            char* key_buf = (char*)malloc(key_length + 1);
            memcpy(key_buf, key + VARCHAR_LENGTH_SIZE, key_length);
            key_buf[key_length] = '\0';

            int cmp = strcmp(key_buf, buf);
            if (cmp < 0) {
                return -1;
            }
            else if (cmp > 0) {
                return 1;
            }
            else {
                return 0;
            }
            break;
    }
    return -2; // If we are thrown an unhandled attribute type
}

// RC IndexManager::compareKey(void *pageData, const void *key, const Attribute &attr, unsigned offset){
//     switch(attr.type){
//         case TypeInt: {
//             int entryKey;
//             memcpy(&entryKey, (char*)pageData + offset, sizeof(INT_SIZE));

//             // Comparison of keys
//             int searchKey;
//             memcpy(&searchKey, key, sizeof(INT_SIZE));
//             if (searchKey < entryKey) return -1; 
//             if (searchKey > entryKey) return 1; 
//             return 0; // Returns 0 when keys match, shouldn't happen
//         }

//         case TypeReal: {
//             float entryKey;
//             memcpy(&entryKey, (char*)pageData + offset, sizeof(REAL_SIZE));
        
//             // Comparison of keys
//             float searchKey;
//             memcpy(&searchKey, key, sizeof(REAL_SIZE));
//             if (searchKey < entryKey) return -1; 
//             if (searchKey > entryKey) return 1; 
//             return 0; // Returns 0 when keys match, shouldn't happen
//         }

//         case TypeVarChar: {
//             // Get position of local varchar
//             int localVarcharOffset;
//             memcpy(&localVarcharOffset, (char*)pageData + offset, sizeof(INT_SIZE));

//             // Access the varchar data using the offset
//             int localVarcharLength;
//             memcpy(&localVarcharLength, (char*)pageData + localVarcharOffset, sizeof(INT_SIZE));
//             char *localString = (char*)malloc(localVarcharLength + 1);
//             memcpy(localString, (char*)pageData + localVarcharOffset + sizeof(INT_SIZE), localVarcharLength);
//             localString[localVarcharLength] = '\0'; // Null terminate the local string

//             // Read the input varchar's length and data
//             int inputVarcharLength;
//             memcpy(&inputVarcharLength, key, sizeof(INT_SIZE));
//             char *inputString = (char*)malloc(inputVarcharLength + 1);
//             memcpy(inputString, (char*)key + sizeof(INT_SIZE), inputVarcharLength);
//             inputString[inputVarcharLength] = '\0'; // Null terminate the input string

//             // Compare the two strings
//             int result = strcmp(localString, inputString);
//             free(localString);
//             free(inputString);
//             return (result < 0) ? -1 : (result > 0) ? 1 : 0; // Limit return to -1,0,1 so we can send error code for unhandled attribute type
//         }
//     }

//     return -2; // If we are thrown an unhandled attribute type
// }

bool IndexManager::fileExists(const string &fileName)
{
    // If stat fails, we can safely assume the file doesn't exist
    struct stat sb;
    return stat(fileName.c_str(), &sb) == 0;
}

void IndexManager::newPageFromEntries(void *oldPageData, void *newPageData, uint32_t startEntry, uint32_t numEntries, bool isTypeVarChar)
{
    memcpy(
            (char *)newPageData + sizeof(IndexHeader),
            (char *)oldPageData + sizeof(IndexHeader) + (startEntry * sizeof(IndexDataEntry)),
            numEntries * sizeof(IndexDataEntry)
          );

    if (!isTypeVarChar)
        return;

    IndexDataEntry oldDataEntry;
    IndexDataEntry newDataEntry;
    int length;
    int totalLength;
    uint16_t freeSpaceOffset = PAGE_SIZE;
    for (uint32_t i = 0; i < numEntries; i++) {
        oldDataEntry = getIndexDataEntry(oldPageData, startEntry + i);
        memcpy(&length, (char *)oldPageData + oldDataEntry.key, sizeof(int));
        totalLength = sizeof(int) + length;
        freeSpaceOffset -= totalLength;
        memcpy(
                (char *)newPageData + freeSpaceOffset,
                (char *)oldPageData + oldDataEntry.key,
                totalLength
              );

        newDataEntry = getIndexDataEntry(newPageData, i);
        newDataEntry.key = freeSpaceOffset;
        setIndexDataEntry(newPageData, i, newDataEntry);
    }
}

bool IndexManager::isValidAttribute(const Attribute &attr)
{
    switch (attr.type) {
      case TypeInt:
        return true;
      case TypeReal:
        return true;
      case TypeVarChar:
        return true;
      default:
        return false;
    }
}

RC IndexManager::deleteInLeaf(void *pageData, unsigned pageNum, const Attribute &attr, uint32_t entryNumber, IXFileHandle &fileHandle)
{
    IndexHeader header = getIndexHeader(pageData);
    unsigned entryOffset = sizeof(IndexHeader) + entryNumber * sizeof(IndexDataEntry);
    IndexDataEntry entryToDelete = getIndexDataEntry(pageData, entryNumber);

    // Handle deletion of the entry and shift any following entries to the left.
    unsigned nextEntryOffset = entryOffset + sizeof(IndexDataEntry);
    // We add -1 to account for 0-indexed entryNumber.
    unsigned entriesToShiftSize = (header.dataEntryNumber - entryNumber - 1) * sizeof(IndexDataEntry);
    header.dataEntryNumber--;
    if (entriesToShiftSize > 0) {
        memmove((char*)pageData + entryOffset, (char*)pageData + nextEntryOffset, entriesToShiftSize);
    } else {
        memset((char *)pageData + entryOffset, 0, sizeof(IndexDataEntry));
    }

    if (attr.type == TypeVarChar) {
        int varcharOffset = entryToDelete.key;
        int varcharLength;
        memcpy(&varcharLength, (char*)pageData + varcharOffset, VARCHAR_LENGTH_SIZE);
        int totalVarcharLength = VARCHAR_LENGTH_SIZE + varcharLength;

        // Shift varchar data to cover the deleted entry's varchar data
        char *startOfShift = (char*)pageData + header.freeSpaceOffset;
        memmove(startOfShift + totalVarcharLength, startOfShift, varcharOffset - header.freeSpaceOffset);

        header.freeSpaceOffset += totalVarcharLength;

        // Update the offsets of remaining varchars in the entries
        for (unsigned i = 0; i < header.dataEntryNumber; i++) {
            IndexDataEntry updatedEntry = getIndexDataEntry(pageData, i);
            if (updatedEntry.key < varcharOffset) {
                updatedEntry.key += totalVarcharLength;
                setIndexDataEntry(pageData, i, updatedEntry);
            }
        }
    }

    setIndexHeader(pageData, header);
    if (fileHandle.writePage(pageNum, pageData))
        return IX_WRITE_FAILED;
  
    return SUCCESS;
}

void IXFileHandle::setfd(FILE *fd)
{
    _fd = fd;
}

FILE *IXFileHandle::getfd()
{
    return _fd;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
    // If pageNum doesn't exist, error
    if (getNumberOfPages() < pageNum)
        return FH_PAGE_DN_EXIST;

    // Try to seek to the specified page
    if (fseek(_fd, PAGE_SIZE * pageNum, SEEK_SET))
        return FH_SEEK_FAILED;

    // Try to read the specified page
    if (fread(data, 1, PAGE_SIZE, _fd) != PAGE_SIZE)
        return FH_READ_FAILED;

    ixReadPageCounter++;
    return SUCCESS;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
    // Check if the page exists
    if (getNumberOfPages() < pageNum)
        return FH_PAGE_DN_EXIST;

    // Seek to the start of the page
    if (fseek(_fd, PAGE_SIZE * pageNum, SEEK_SET))
        return FH_SEEK_FAILED;

    // Write the page
    if (fwrite(data, 1, PAGE_SIZE, _fd) == PAGE_SIZE)
    {
        // Immediately commit changes to disk
        fflush(_fd);
        ixWritePageCounter++;
        return SUCCESS;
    }
    
    return FH_WRITE_FAILED;
}

RC IXFileHandle::appendPage(const void *data)
{
    // Seek to the end of the file
    if (fseek(_fd, 0, SEEK_END))
        return FH_SEEK_FAILED;

    // Write the new page
    if (fwrite(data, 1, PAGE_SIZE, _fd) == PAGE_SIZE)
    {
        fflush(_fd);
        ixAppendPageCounter++;
        return SUCCESS;
    }

    return FH_WRITE_FAILED;
}

unsigned IXFileHandle::getNumberOfPages()
{
    // Use stat to get the file size
    struct stat sb;
    if (fstat(fileno(_fd), &sb) != 0)
        // On error, return 0
        return 0;
    // Filesize is always PAGE_SIZE * number of pages
    return sb.st_size / PAGE_SIZE;
}
