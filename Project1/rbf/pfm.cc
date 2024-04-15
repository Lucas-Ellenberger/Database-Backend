#include <sys/stat.h>
#include <iostream>
#include <cmath>

#include "pfm.h"

using std::cin;
using std::cout;
using std::cerr;
using std::endl;


PagedFileManager *PagedFileManager::_pf_manager = 0;

PagedFileManager *PagedFileManager::instance()
{
    if (!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

PagedFileManager::PagedFileManager()
{
    // Initialize a header page for the heap file.
    // Each block in the header page will store the following:
    // (Pointer to the Data Page, Amount of free space on DP)
    // The pointer can be the offset.
    // We will eventually need to have a pointer to the next page in the header page.
}

PagedFileManager::~PagedFileManager()
{
    // Destroy the header page for the heap file.
}

RC PagedFileManager::createFile(const string &fileName)
{
    // Check if the fileName already exists.
    struct stat fileAtt;
    if (stat(fileName.c_str(), &fileAtt) == 0)
    {
        cerr << "File already Exists\n";
        return 1;
    }

    FILE *file = fopen(fileName.c_str(), "wb");
    if (file == NULL)
    {
        cerr << "Error in creating file\n";
        return 2;
    }

    fclose(file);
    return 0;
}

RC PagedFileManager::destroyFile(const string &fileName)
{
    // Check if the filename exists.
    struct stat fileAtt;
    if (stat(fileName.c_str(), &fileAtt) != 0)
    {
        cerr << "Error obtaining file\n";
        return 1;
    }

    if (remove(fileName.c_str()) != 0)
    {
        cerr << "Error in removing file\n";
        return 2;
    }

    return 0;
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    // Check if the fileName exists.
    struct stat fileAtt;
    if (stat(fileName.c_str(), &fileAtt) != 0)
    {
        if (errno == ENOENT)
        {
            std::cerr << "File does not exist\n";
            return 1;
        }

        cerr << "Error Occured\n";
        return 2;
    };

    // Check if the given FileHandle Object already has an open file.
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

    // Find the number of pages in the file.
    fseek(fileHandle.file, 0, SEEK_END);
    fileHandle.pagecount = ftell(fileHandle.file) >> PAGE_SHIFT;

    return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if (fileHandle.file == nullptr)
    {
        cerr << "fileHandle object does not have an open file\n";
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
    // User must close any files held by pointers.
    if(this->file != nullptr) {
        // FileHandle has an open file associated with it.
        cerr << "File not closed, cannot remove file handle" << endl;
    }
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
    // Write the data from the given page number into *data.
    if (this->pagecount <= pageNum) {
        cerr << "page number requested is greater than the number of pages for this file" << endl;
        return -1;
    }

    // Every page is 4096 bytes, so we can shift the page number left by 12 bits to access the data.
    unsigned offset = pageNum << PAGE_SHIFT; // Multiply pageNum by 4096 to get to correct page.

    fseek(this->file, offset, SEEK_SET); // Set the file pointer to the correct spot in the page.
    fread(data, 1, PAGE_SIZE, this->file); // Read the data.

    this->readPageCounter += 1;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    // Given the page num, write *data into the page.
    if (pageNum > this->pagecount) {
        cerr << "page number requested is greater than the number of pages for this file" << endl;
        return -1;
    }
    if (pageNum == this->pagecount) {
        this->writePageCounter += 1;
        return appendPage(data);
    }

    unsigned offset = pageNum << PAGE_SHIFT; // Multiply pageNum by 4096 to get to correct page.

    fseek(this->file, offset, SEEK_SET); // Set the file pointer to the correct spot in the file.
    fwrite(data, 1, PAGE_SIZE, this->file); // Write the data.

    this->writePageCounter += 1;
    return 0;
}

RC FileHandle::appendPage(const void *data)
{
    unsigned offset = this->pagecount << PAGE_SHIFT; // Multiply number of pages by 4096 to get to correct page.

    fseek(this->file, offset, SEEK_SET); // Set the file pointer to the correct spot in the file.
    fwrite(data, 1, PAGE_SIZE, this->file); // Write the data.

    this->appendPageCounter += 1;
    this->pagecount += 1;
    return 0;
}

unsigned FileHandle::getNumberOfPages()
{
    return pagecount;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return 0;
}
