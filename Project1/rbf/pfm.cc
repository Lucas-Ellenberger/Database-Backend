#include <sys/stat.h>
#include <iostream>

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
    {
        std::cerr << "File already Exists\n";
        return 1;
    }

    FILE *file = fopen(fileName.c_str(), "wb");
    if (file == NULL)
    {
        std::cerr << "Error in creating file\n";
        return 2;
    }
    fclose(file);
    return 0;
}

RC PagedFileManager::destroyFile(const string &fileName)
{
    // Checks if filename exists
    struct stat fileAtt;
    if (stat(fileName.c_str(), &fileAtt) != 0)
    {
        std::cerr << "Error obtaining file\n";
        return 1;
    }
    if (remove(fileName.c_str()) != 0)
    {
        std::cerr << "Error in removing file\n";
        return 2;
    }
    return 0;
}
RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    // Check if fileName exists
    struct stat fileAtt;
    if (stat(fileName.c_str(), &fileAtt) != 0)
    {
        if (errno == ENOENT)
        {
            std::cerr << "File does not exist\n";
            return 1;
        }
        std::cerr << "Error Occured\n";
        return 2;
    };

    // Check if given FileHandle Object already has an open file
    if (fileHandle.file != nullptr)
    {
        std::cerr << "FileHandle object already has an open file\n";
        return 3;
    }
    fileHandle.file = fopen(fileName.c_str(), "rb+");
    if (fileHandle.file == NULL)
    {
        std::cerr << "Error in opening file\n";
        return 4;
    }
    return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if (fileHandle.file == nullptr)
    {
        std::cerr << "fileHandle object does not have an open file\n";
        return 1;
    }
    fclose(fileHandle.file);
    fileHandle.file = nullptr;
    return 0;
}

FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    pagecount = 0;
    file = nullptr;
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
    return pagecount;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return -1;
}
