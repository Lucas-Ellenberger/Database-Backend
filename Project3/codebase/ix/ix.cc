#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>

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
    // Initialize the internal IndexManager instance.
    _pf_manager = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return IX_CREATE_FAILED;

    // Setting up the header page.
    void * headerPageData = calloc(PAGE_SIZE, 1);
    if (headerPageData == NULL)
        return IX_MALLOC_FAILED;

    // TODO: Implement helper function.
    newHeaderPage(headerPageData);

    // Adds the meta data page.
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return IX_OPEN_FAILED;

    if (handle.appendPage(headerPageData))
        return IX_APPEND_FAILED;

    _pf_manager->closeFile(handle);
    free(headerPageData);

    void * firstInternalPageData = calloc(PAGE_SIZE, 1);
    if (firstInternalPageData == NULL)
        return IX_MALLOC_FAILED;
    
    // TODO: Implement helper function.
    newInternalPage(firstInternalPageData, -1/* leftChildPageNum */);

    if (handle.appendPage(firstInternalPageData))
        return IX_APPEND_FAILED;

    _pf_manager->closeFile(handle);
    free(firstInternalPageData);

    return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return _pf_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return _pf_manager->openFile(fileName.c_str(), ixfileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return _pf_manager->closeFile(ixfileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{  
    // First create data entry to be inserted
    IndexDataEntry newDataEntry;
    newDataEntry.key = NULL; 
    newDataEntry.rid;

    unsigned rootPageNum = getRootPage(ixfileHandle); // TO-DO: need to implement getRootPage(), should retrieve the root page from meta data page

    return insert(attribute, key, rid, ixfileHandle, newDataEntry, rootPageNum); // Will recursively walk down tree until it finds leaf page to insert
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
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

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
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
    memcpy(pageData, &indexHeader, sizeof(IndexHeader));
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
            ((char *) pageData + sizeof(IndexHeader) + (indexEntryNumber * sizeof(IndexDataEntry))),
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
    return indexHeader.freeSpaceOffset - (indexHeader.dataEntryNumber * sizeof(IndexDataEntry) - sizeof(IndexHeader));
}

RC IndexManager::insert(const Attribute &attr, const void *key, const RID &rid, IXFileHandle &fileHandle, IndexDataEntry &newIndexDataEntry, unsigned pageNum){
    void *pageData = malloc(PAGE_SIZE);

    if (fileHandle.readPage(pageNum, pageData) != SUCCESS){ // Assuming that IXFileHandle has identical methods as other FileHandle class
        free(pageData);
        return IX_READ_FAILED;
    }
    
    // Checks if current page is a leaf or internal page
    if (isNonLeaf(pageData)){ // If internal page, get child page, and recursively call insert
        unsigned childPageNum = getChildPageNum(pageData, key, attr); // TO-DO: Should look through data entries by corresponding key value and grab child page num
        RC rc = insert(attr, key, rid, fileHandle, newIndexDataEntry, childPageNum); 
        if (newIndexDataEntry.key != NULL){
            rc = insertInInternal(pageData, pageNum, newIndexDataEntry); // This will be called at every backtrack level until we find space to insert 
            if (rc == IX_INTERNAL_SPLIT){
                rc = splitInternal(pageData, newIndexDataEntry);
            }
        }
    } else { // If leaf page, attempt to insert data entry in leaf
        RC rc = insertInLeaf(attr, key, rid, pageData); // TO-DO: Should return 0 if successfully inserted, return IX_LEAF_SPLIT if split is needed 
        if (rc == IX_LEAF_SPLIT) {
            rc = splitLeaf(pageData, newIndexDataEntry); // TO-DO: Should split leaf page into two, find in parent if needed.
        }
        fileHandle.writePage(pageNum, pageData);
    }

    free(pageData);
    return SUCCESS;
}

unsigned IndexManager::getRootPage(IXFileHandle &fileHandle){
    // Should grab root page from meta data page
}

bool IndexManager::isNonLeaf(void *pageData){
    // Should check flag of current page, return true if page is non-leaf and false if page is leaf
}

unsigned IndexManager::getChildPageNum(void *pageData, const void *key, const Attribute &attr){
    // Should look through internal data entries by key and return child page num
}

RC IndexManager::insertInInternal(void *pageData, unsigned pageNum, IndexDataEntry &newIndexDataEntry){
    // Should attempt to place internal "traffic cop" within page. If successful, set key within data entry to null 
}

RC IndexManager::splitInternal(void*pageData, IndexDataEntry &newIndexDataEntry){
    // Should split internal page into two
}

RC IndexManager::insertInLeaf(const Attribute &attr, const void *key, const RID &rid, void *pageData){
    // Should return SUCCESS if new data entry is inserted into leaf page. If page is full, return IX_INSERT_SPLIT 
}

RC IndexManager::splitLeaf(void *pageData, IndexDataEntry &newIndexDataEntry){
    // Should split leaf into two, insert the newIndexDataEntry, and pass middle key value back in struct to signify split.
}