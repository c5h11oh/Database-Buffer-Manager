/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	delete [] hashTable;
	delete [] bufPool;
	delete [] bufDescTable;
}

void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
	// check if all buffer is pinned
	bool allIsPinned = true;
	for(uint i = 0; i < numBufs; ++i){
		if (bufDescTable[i].pinCnt == 0){
			allIsPinned = false;
			break;
		}
	}
	if(allIsPinned) {
		throw BufferExceededException(); 
		return;
	}

	while(true){
		advanceClock();
		// if valid bit is not set, found frame.
		if(!bufDescTable[clockHand].valid){
			frame = clockHand;
			return;
		}
        
		
		// if refbit is set, unset it and restart the loop
		if(bufDescTable[clockHand].refbit){
			bufDescTable[clockHand].refbit = false;
			continue;
		}

		// if page is pinned (pinCnt != 0), restart the loop. else we have found the frame
		if(bufDescTable[clockHand].pinCnt){
			continue;
		}

		// if dirty, we need to write back data first
		if(bufDescTable[clockHand].dirty){
			bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
		}

		// found frame
		frame = clockHand;
		return; // Set() should be called by the caller, such as readPage()
	}
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{   
    //We want to first check if this page is already in the buffer pool
    FrameId fId;
    try 
    {
        hashTable.lookup(file, pageNo, fId);
    }

    //The page does not exist in the buffer pool
    catch (HashNotFoundException e) 
    {
    //Call allocBuf() to allocate a buffer frame
    const FrameID returnValue;
    allocBuf(returnValue);
    //Call the method file->readPage() to read the page from disk into the buffer pool frame
    bufPool[returnValue] = file->readPage(returnValue);
    //Insert the page into the hashtable
    hashTable.insert(file, pageNo, returnValue);
    //Invoke Set() on the frame to set it up properly
    bufDescTable[returnValue].Set(file, PageNo);
    //Return a pointer to the frame containing the page via the page parameter
    page = &(bufPool[returnValue]);
    return; 
    }

    //The page exists in the buffer pool
    //Set the appropriate refbit
    bufDescTable[PageNo].refbit = true;
    //Increment the pinCnt for the page
    bufDescTable[PageNo].pinCnt++;
    //Return a pointer to the frame containing the page via the page parameter
    page = &(bufPool[fId]); // the "return" is here
    return;
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
}

void BufMgr::flushFile(const File* file) 
{
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
