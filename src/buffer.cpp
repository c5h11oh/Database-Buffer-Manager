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

		// remove old hash table entry
		hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);

		// return frame
		frame = clockHand;
		return; // It's the caller's (e.g. readPage()'s) responsibility to insert new hashTable entry and call Set().
	}
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo = 0;
	try{
		hashTable->lookup(file, pageNo, frameNo);
		if(bufDescTable[frameNo].pinCnt == 0)
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		--bufDescTable[frameNo].pinCnt;
		if(dirty)
			bufDescTable[frameNo].dirty = true;
	}
	catch(HashNotFoundException& e){
		return;
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	
	FrameId fId;
	allocBuf(fId);

	Page p = file->allocatePage();
	pageNo = p.page_number();
	hashTable->insert(file, pageNo, fId);
	bufDescTable[fId].Set(file, pageNo);
	bufPool[fId] = p;
	page = &(bufPool[fId]);

	return;
}

void BufMgr::flushFile(File* file) 
{
	for(FrameId i = 0; i < numBufs; ++i){
		if(bufDescTable[i].file == file){
			if(bufDescTable[i].dirty){
				file->writePage(bufPool[i]);
			}

		}
	}
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
