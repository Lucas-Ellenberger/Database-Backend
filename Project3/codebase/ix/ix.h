#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_CREATE_FAILED   10
# define IX_MALLOC_FAILED   11
# define IX_OPEN_FAILED     12
# define IX_APPEND_FAILED   13
# define IX_READ_FAILED     14
# define IX_WRITE_FAILED    15
# define IX_SLOT_DN_EXIST   16
# define IX_READ_AFTER_DEL  17
# define IX_NO_SUCH_ATTR    18
# define IX_LEAF_SPLIT      19
# define IX_INTERNAL_SPLIT  20
# define IX_EXISTING_ENTRY  21
# define IX_FILE_DN_EXIST   22
# define IX_HANDLE_IN_USE   23
# define IX_FILE_EXISTS     24
# define IX_REMOVE_FAILED   25

# define IX_EOF (-1)  // end of the index scan

typedef struct MetaDataHeader
{
    uint32_t rootPageNum;
} MetaDataHeader;

typedef struct IndexHeader
{
    bool leaf;
    uint32_t dataEntryNumber;
    uint32_t nextSiblingPageNum;
    uint32_t prevSiblingPageNum;
    uint32_t leftChildPageNum;
    uint16_t freeSpaceOffset;
} IndexHeader;

typedef struct IndexDataEntry
{
    // MUST CAST TO FLOAT WHEN NEEDED!
    int key;
    RID rid;
} IndexDataEntry;

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        
        void newHeaderPage(void* pageData);
        void setMetaDataHeader(void *pageData, MetaDataHeader metaHeader);
        MetaDataHeader getMetaDataHeader(void *pageData);
        void newInternalPage(void* pageData, int leftChildPageNum);
        void newLeafPage(void *pageData, int nextSiblingPageNum, int prevSiblingPageNum);
        void setIndexHeader(void *pageData, IndexHeader indexHeader);
        IndexHeader getIndexHeader(void *pageData);
        void setIndexDataEntry(void *pageData, unsigned indexEntryNumber, IndexDataEntry dataEntry);
        IndexDataEntry getIndexDataEntry(void *pageData, unsigned indexEntryNumber);
        unsigned getPageFreeSpaceSize(void *pageData);
        RC insert(const Attribute &attr, const void *key, const RID &rid, IXFileHandle &fileHandle, IndexDataEntry &newIndexDataEntry, unsigned pageNum);
        unsigned getRootPage(IXFileHandle &fileHandle);
        bool isNonLeaf(void *pageData);
        unsigned getChildPageNum(void *pageData, const void *key, const Attribute &attr);
        RC insertInInternal(void *pageData, unsigned pageNum, IndexDataEntry &newIndexDataEntry);
        RC insertInLeaf(const Attribute &attr, const void *key, const RID &rid, void *pageData);
        RC splitLeaf(void *pageData, IndexDataEntry &newIndexDataEntry);
        RC splitInternal(void *pageData, IndexDataEntry &newIndexDataEntry);
        RC compareKey(void *pageData, const void *key, const Attribute &attr, unsigned offset);
        bool fileExists(const string &fileName);

        RC findOptimalPage(const Attribute &attr, const void* key, IXFileHandle &fileHandle);
        RC optimalPageHelper(const Attribute &attr, const void* key, IXFileHandle &fileHandle, uint32_t pageNum);
};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();


    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    friend class IndexManager;
    private:
    FILE *_fd;

    // Private helper methods
    void setfd(FILE *fd);
    FILE *getfd();

};

#endif
