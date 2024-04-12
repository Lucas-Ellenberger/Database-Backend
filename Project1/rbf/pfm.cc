#include <sys/stat.h>
#include <iostream>

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
    // Checks if filename exists
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
    // Check if fileName exists
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
    // need to destroy any files held by pointers
    if(this->file != nullptr) {
        //has file associated with it and file is still open. 
        cerr << "File not closed, cannot remove file handle" << endl;
    }
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
    //given the page num, modify the memory block at *data to have all the information stored the page pageNum
    if (this->pagecount <= pageNum) {
        cerr << "page number requested is greater than the number of pages for this file" << endl;
        return -1;
    }

    //given that any page is 4096 bytes, then we should be able to just say "page 0 starts at byte 0 ends at 4095, page 1 starts at 4096... "
    unsigned offset = pageNum << 12; //multiply pageNum by 4096 to get to correct page

    fseek(this->file, offset, SEEK_SET); // set us to the correct spot in the file for the requested page
    fread(data, 1, 4096, this->file); //read the data


    this->readPageCounter += 1;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    //given the page num, modify the page to have all the data pointed to by data
    if (pageNum > this->pagecount) {
        cerr << "page number requested is greater than the number of pages for this file" << endl;
        return -1;
    }
    if (pageNum == this->pagecount) {
        this->writePageCounter += 1;
        return appendPage(data);
    }

    unsigned offset = pageNum << 12; //multiply pageNum by 4096 to get to correct page

    fseek(this->file, offset, SEEK_SET); // set us to the correct spot in the file for the requested page
    fwrite(data, 1, 4096, this->file); //write the data

    this->writePageCounter += 1;
    return 0;
}

RC FileHandle::appendPage(const void *data)
{
    unsigned offset = this->pagecount << 12; //multiply number of pages by 4096 to get to correct page

    fseek(this->file, offset, SEEK_SET); // set us to the correct spot in the file for the requested page
    fwrite(data, 1, 4096, this->file); //write the data

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
