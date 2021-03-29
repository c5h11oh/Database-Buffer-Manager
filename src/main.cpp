/**
 * Ismael Jaffri ijaffri
 * Wuh-Chwen Hwang whwang8
 * Matthew Thomas mcthomas4
 *  
 * This is the driver (test cases) of the BadgerDB. It tests the file I/O, reading/writing pages, 
 * and manipulating records. It also tests the buffer manager's APIs (public functions) by creating 
 * a BufMgr and make CRUD accesses. All codes are given by the assignment.
 * 			
 */

#include <iostream>
#include <fstream>
#include <algorithm>
#include <stdlib.h>
//#include <stdio.h>
#include <cstring>
#include <memory>
#include "page.h"
#include "buffer.h"
#include "file_iterator.h"
#include "page_iterator.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/invalid_page_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/insufficient_space_exception.h"

#define PRINT_ERROR(str) \
{ \
	std::cerr << "On Line No:" << __LINE__ << "\n"; \
	std::cerr << str << "\n"; \
	exit(1); \
}

using namespace badgerdb;

const PageId num = 100;
PageId pid[num], pageno1, pageno2, pageno3, i;
RecordId rid[num], rid2, rid3;
Page *page, *page2, *page3;
char tmpbuf[100];
BufMgr* bufMgr;
File *file1ptr, *file2ptr, *file3ptr, *file4ptr, *file5ptr, *svenptr;

void test1();
void test2();
void test3();
void test4();
void test5();
void test6();
void test8();
void testBufMgr();

int main() 
{
	//Following code shows how to you File and Page classes

  const std::string& filename = "test.db";
  // Clean up from any previous runs that crashed.
  try
	{
    File::remove(filename);
  }
	catch(const FileNotFoundException &)
	{
  }

  {
    // Create a new database file.
    File new_file = File::create(filename);
    
    // Allocate some pages and put data on them.
    PageId third_page_number;
    for (int i = 0; i < 5; ++i) {
      Page new_page = new_file.allocatePage();
      if (i == 3) {
        // Keep track of the identifier for the third page so we can read it
        // later.
        third_page_number = new_page.page_number();
      }
      new_page.insertRecord("hello!");
      // Write the page back to the file (with the new data).
      new_file.writePage(new_page);
    }

    // Iterate through all pages in the file.
    for (FileIterator iter = new_file.begin();
         iter != new_file.end();
         ++iter) {

			Page curr_page = (*iter);
      // Iterate through all records on the page.
      for (PageIterator page_iter = curr_page.begin();
           page_iter != curr_page.end();
           ++page_iter) {
        std::cout << "Found record: " << *page_iter
            << " on page " << curr_page.page_number() << "\n";
      }
    }

    // Retrieve the third page and add another record to it.
    Page third_page = new_file.readPage(third_page_number);
    const RecordId& rid = third_page.insertRecord("world!");
    new_file.writePage(third_page);

    // Retrieve the record we just added to the third page.
    std::cout << "Third page has a new record: "
        << third_page.getRecord(rid) << "\n\n";
  }
  // new_file goes out of scope here, so file is automatically closed.

  // Delete the file since we're done with it.
  File::remove(filename);

	//This function tests buffer manager, comment this line if you don't wish to test buffer manager
	testBufMgr();
}

void testBufMgr()
{
	// create buffer manager
	bufMgr = new BufMgr(num);

	// create dummy files
  const std::string& filename1 = "test.1";
  const std::string& filename2 = "test.2";
  const std::string& filename3 = "test.3";
  const std::string& filename4 = "test.4";
  const std::string& filename5 = "test.5";
//   const std::string& filenamesven = "test.sven";

  try
	{
    File::remove(filename1);
    File::remove(filename2);
    File::remove(filename3);
    File::remove(filename4);
    File::remove(filename5);
	File::remove("test.sven1");
	File::remove("test.sven2");

  }
	catch(const FileNotFoundException &e)
	{
  }

	File file1 = File::create(filename1);
	File file2 = File::create(filename2);
	File file3 = File::create(filename3);
	File file4 = File::create(filename4);
	File file5 = File::create(filename5);
	// File filesven = File::create(filenamesven);

	file1ptr = &file1;
	file2ptr = &file2;
	file3ptr = &file3;
	file4ptr = &file4;
	file5ptr = &file5;
	// svenptr = &filesven;

	//Test buffer manager
	//Comment tests which you do not wish to run now. Tests are dependent on their preceding tests. So, they have to be run in the following order. 
	//Commenting  a particular test requires commenting all tests that follow it else those tests would fail.
	test1();
	test2();
	test3();
	test4();
	test5();
	test6();
	
	delete bufMgr;
	bufMgr = new BufMgr(num);
	file1.~File();
	test8();
	file1 = File::open(filename1);

	//Close files before deleting them
	file1.~File();
	file2.~File();
	file3.~File();
	file4.~File();
	file5.~File();

	//Delete files
	File::remove(filename1);
	File::remove(filename2);
	File::remove(filename3);
	File::remove(filename4);
	File::remove(filename5);
	// File::remove(filenamesven);

	delete bufMgr;

	std::cout << "\n" << "Passed all tests." << "\n";
}

void test1()
{
	//Allocating pages in a file...
	for (i = 0; i < num; i++)
	{
		bufMgr->allocPage(file1ptr, pid[i], page);
		sprintf((char*)tmpbuf, "test.1 Page %u %7.1f", pid[i], (float)pid[i]);
		rid[i] = page->insertRecord(tmpbuf);
		bufMgr->unPinPage(file1ptr, pid[i], true);
	}

	//Reading pages back...
	for (i = 0; i < num; i++)
	{
		bufMgr->readPage(file1ptr, pid[i], page);
		sprintf((char*)&tmpbuf, "test.1 Page %u %7.1f", pid[i], (float)pid[i]);
		if(strncmp(page->getRecord(rid[i]).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}
		bufMgr->unPinPage(file1ptr, pid[i], false);
	}
	std::cout<< "Test 1 passed" << "\n";
}

void test2()
{
	//Writing and reading back multiple files
	//The page number and the value should match

	for (i = 0; i < num/3; i++) 
	{
		bufMgr->allocPage(file2ptr, pageno2, page2);
		sprintf((char*)tmpbuf, "test.2 Page %u %7.1f", pageno2, (float)pageno2);
		rid2 = page2->insertRecord(tmpbuf);

		long int index = random() % num;
    	pageno1 = pid[index];
		bufMgr->readPage(file1ptr, pageno1, page);
		sprintf((char*)tmpbuf, "test.1 Page %u %7.1f", pageno1, (float)pageno1);
		if(strncmp(page->getRecord(rid[index]).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}

		bufMgr->allocPage(file3ptr, pageno3, page3);
		sprintf((char*)tmpbuf, "test.3 Page %u %7.1f", pageno3, (float)pageno3);
		rid3 = page3->insertRecord(tmpbuf);

		bufMgr->readPage(file2ptr, pageno2, page2);
		sprintf((char*)&tmpbuf, "test.2 Page %u %7.1f", pageno2, (float)pageno2);
		if(strncmp(page2->getRecord(rid2).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}

		bufMgr->readPage(file3ptr, pageno3, page3);
		sprintf((char*)&tmpbuf, "test.3 Page %u %7.1f", pageno3, (float)pageno3);
		if(strncmp(page3->getRecord(rid3).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}

		bufMgr->unPinPage(file1ptr, pageno1, false);
	}

	for (i = 0; i < num/3; i++) {
		bufMgr->unPinPage(file2ptr, i+1, true);
		bufMgr->unPinPage(file2ptr, i+1, true);
		bufMgr->unPinPage(file3ptr, i+1, true);
		bufMgr->unPinPage(file3ptr, i+1, true);
	}

	std::cout << "Test 2 passed" << "\n";
}

void test3()
{
	try
	{
		bufMgr->readPage(file4ptr, 1, page);
		PRINT_ERROR("ERROR :: File4 should not exist. Exception should have been thrown before execution reaches this point.");
	}
	catch(const InvalidPageException &e)
	{
	}

	std::cout << "Test 3 passed" << "\n";
}

void test4()
{
	bufMgr->allocPage(file4ptr, i, page);
	bufMgr->unPinPage(file4ptr, i, true);
	try
	{
		bufMgr->unPinPage(file4ptr, i, false);
		PRINT_ERROR("ERROR :: Page is already unpinned. Exception should have been thrown before execution reaches this point.");
	}
	catch(const PageNotPinnedException &e)
	{
	}

	std::cout << "Test 4 passed" << "\n";
}

void test5()
{
	for (i = 0; i < num; i++) {
		bufMgr->allocPage(file5ptr, pid[i], page);
		sprintf((char*)tmpbuf, "test.5 Page %u %7.1f", pid[i], (float)pid[i]);
		rid[i] = page->insertRecord(tmpbuf);
	}

	PageId tmp;
	try
	{
		bufMgr->allocPage(file5ptr, tmp, page);
		PRINT_ERROR("ERROR :: No more frames left for allocation. Exception should have been thrown before execution reaches this point.");
	}
	catch(const BufferExceededException &e)
	{
	}

	std::cout << "Test 5 passed" << "\n";

	for (i = 1; i <= num; i++)
		bufMgr->unPinPage(file5ptr, i, true);
}

void test6()
{
	//flushing file with pages still pinned. Should generate an error
	for (i = 1; i <= num; i++) {
		bufMgr->readPage(file1ptr, i, page);
	}

	try
	{
		bufMgr->flushFile(file1ptr);
		PRINT_ERROR("ERROR :: Pages pinned for file being flushed. Exception should have been thrown before execution reaches this point.");
	}
	catch(const PagePinnedException &e)
	{
	}

	std::cout << "Test 6 passed" << "\n";

	for (i = 1; i <= num; i++) 
		bufMgr->unPinPage(file1ptr, i, true);

	bufMgr->flushFile(file1ptr);
}

int hashFunction(int in, int size = 8193){
	return (in * 21017 + 4027) % size;
}

inline bool same(std::istream& in1, std::istream& in2){
	in1.seekg(0, std::istream::beg);
	in2.seekg(0, std::istream::beg);
	return std::equal(std::istreambuf_iterator<char>(in1.rdbuf()),
							std::istreambuf_iterator<char>(), 
							std::istreambuf_iterator<char>(in2.rdbuf()));
}

std::string blankStr("");
void assertSame(std::istream& in1, std::istream& in2, std::string&& err_msg = "", bool diff = false){
	bool result = same(in1, in2);
	if(diff && result){
		PRINT_ERROR( ("ERROR: SVEN: file should be different but it's same. MSG:" + err_msg) );
	}
	else if(!result){
		PRINT_ERROR( ("ERROR: SVEN: file should be same but it's different. MSG:" + err_msg) );
	}
}

void test8(){
	// testing the persistence is really done
		// Copy test.1 to test.sven1,2
		// std::ifstream  src("test.1", std::ios::binary);
		// std::fstream  dst1("test.sven1", std::ios::binary | std::ios::trunc);
		// std::fstream  dst2("test.sven2", std::ios::binary | std::ios::trunc);
		// src.seekg(0, std::ifstream::beg);
		// dst1 << src.rdbuf();
		// src.seekg(0, std::ifstream::beg);
		// dst2 << src.rdbuf();
		/* https://stackoverflow.com/questions/10195343/copy-a-file-in-a-sane-safe-and-efficient-way */
		
		// Make sure they're identical 
		std::fstream sven1("test.sven1", std::ios::binary), 
					 sven2("test.sven2", std::ios::binary);
		// assertSame(src, sven1);
		// assertSame(src, sven2);
		// src.close();
		/* https://stackoverflow.com/questions/6163611/compare-two-files/37575457 */
		
		File filesven1 = File::open("test.sven1");
		File filesven2 = File::open("test.sven2");
		
	// 1. sven1: if there are 100 pages edited, the underlying file will not change 
	//	  because the buffer has size 100 pages
		for(uint i = 0; i < num; ++i){
			bufMgr->allocPage(&filesven1, pid[i], page);
		}
		sven1.seekg(0, std::ifstream::beg);
		sven2.seekg(0, std::ifstream::beg);
		sven2 << sven1.rdbuf();
		for(uint i = 0; i < num; ++i){
			sprintf((char*)tmpbuf, "test.sven1 Page %u hash %d", pid[i], hashFunction(i));
			rid[i] = page->insertRecord(tmpbuf);
			bufMgr->unPinPage(&filesven1, pid[i], true);
		}
		assertSame(sven1, sven2, "SVEN1");
	
	// 2. if we call flush, the data will write into the file
		bufMgr->flushFile(&filesven1);
		assertSame(sven1, sven2, "SVEN2", true); // I don't know how to do better
	
	PageId bigPid[2 * num];
	RecordId bigRid[2 * num];
	// 3. sven2: if we edit 200 distinct pages, the first 100 will be written in the file
		for(uint i = 0; i < 2 * num; ++i){
			bufMgr->allocPage(&filesven2, bigPid[i], page);
			sprintf((char*)tmpbuf, "test.sven2 Page %u hash %d", bigPid[i], hashFunction(i));
			bigRid[i] = page->insertRecord(tmpbuf);
			bufMgr->unPinPage(&filesven2, bigPid[i], true);
		}
		assertSame(sven1, sven2, std::string("SVEN3"));
	// 4. if we delete bufMgr, the deconstructor will be called and the file will be written
		delete bufMgr;
		bufMgr = new BufMgr(num);
		for(uint i = 0; i < 2 * num; ++i){
			bufMgr->readPage(&filesven2, bigPid[i], page);
			sprintf((char*)tmpbuf, "test.sven2 Page %u hash %d", bigPid[i], hashFunction(i));
			if(strncmp(page->getRecord(bigRid[i]).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
			{
				PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH. SVEN4");
			}			
			bufMgr->unPinPage(&filesven2, bigPid[i], false);
		}
	// 5. the page should eventually be filled and throw InsufficientSpaceException if we continue to write record
		try{
			bufMgr->readPage(&filesven2, bigPid[1], page);
			for(uint i = 0; i < UINT32_MAX; ++i){
				page->insertRecord("Matt, Ismael, and Sven are all handsome guy");
			}
		}
		catch(const InsufficientSpaceException& e){
			return;
		}
		
		PRINT_ERROR("ERROR :: EXPECT AN InsufficientSpaceException. SVEN5");

	// close istream
	sven1.close();
	sven2.close();
}

