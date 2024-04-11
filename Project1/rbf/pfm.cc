#include <sys/stat.h>

#include "pfm.h"

PagedFileManager *PagedFileManager::_pf_manager = 0;

PagedFileManager *PagedFileManager::instance()
{
    if (!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

PagedFileManager::PagedFileManager()
{
}

PagedFileManager::~PagedFileManager()
{
}

RC PagedFileManager::createFile(const string &fileName)
{
    // Checks if fileName already exists
    struct stat fileAtt;
    if (stat(fileName.c_str(), &fileAtt) == 0)
        return 1; // File already exists

    FILE *file = fopen(fileName.c_str(), "wb");
    if (file == NULL)
        return 2; // Error in creating file

    return 0;
}

RC PagedFileManager::destroyFile(const string &fileName)
{
    return -1;
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    // Check if fileName exists
    struct stat fileAtt;
    if (stat(fileName.c_str(), &fileAtt) != 0)
    {
        if (errno == ENOENT)
            return 1; // File does not exist
        return 2;     // A different error occured
    };

    // Check if given FileHandle Object already belongs to an open file

    FILE *file = fopen(fileName.c_str(), "rb+");
    return -1;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    return -1;
}

FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}

FileHandle::~FileHandle()
{
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
    return -1;
}

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    return -1;
}

RC FileHandle::appendPage(const void *data)
{
    return -1;
}

unsigned FileHandle::getNumberOfPages()
{
    return -1;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return -1;
}
