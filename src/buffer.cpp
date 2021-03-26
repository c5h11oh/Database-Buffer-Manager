/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

/**
 * Ismael Jaffri ijaffri
 * Wuh-Chwen Hwang whwang8
 * Matthew Thomas mcthomas4
 *  
 * Managing file for our Buffer Manager. "The central class which manages 
 * the buffer pool including frame allocation and deallocation to pages in the file" 
 * 
 * Controls which pages are memory resident, using the clock algorithm as
 * a buffer replacement policy.
 * 			
 */
#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/invalid_page_exception.h"

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

/**
 * Flushes dirty pages and deallocates the buffer pool (Destructor). 
 *
 */
BufMgr::~BufMgr() {

	//iterate through buffer pool, and write dirty pages to disk
	for(FrameId i = 0; i < numBufs; ++i){
		if(bufDescTable[i].dirty){
			bufDescTable[i].file->writePage(bufPool[i]);
		}
	}
	
	//deallocate objects that were allocated during runtime
	delete hashTable;
	delete [] bufPool;
	delete [] bufDescTable;
}

/**
 * Advances the clock to the next frame of the buffer pool. 
 *
 */
void BufMgr::advanceClock()
{
	//modulo number of bufs, to not exceed numBufs
	clockHand = (clockHand + 1) % numBufs;
}

/**
 * Allocates free frame using clock algorithm, and writes dirty page to disk
 * if necessary. 
 *
 * @param frame Frame reference, frame ID of allocated frame returned via this variable.
 * @throws BufferExceededException Thrown if all buffer frames are pinned.
 *
 */
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
	//BufferExceededException Thrown if all buffer frames are pinned
	if(allIsPinned) {
		throw BufferExceededException(); 
		return;
	}

	while(true){
		advanceClock();
		// if valid bit is not set, found frame
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

		// Clear() the buffer description
		bufDescTable[clockHand].Clear();

		// return frame
		frame = clockHand;
		return; // It's the caller's (e.g. readPage()'s) responsibility to insert new hashTable entry and call Set()
	}
}

/**
 * Checks if page is in the bufferpool, via the lookup() method, and handles
 * the frame appropriately, returning a pointer to the frame.
 *
 * @param file Pointer to file to which corresponding frame is assigned.
 * @param pageNo Page within file to which corresponding frame is assigned.
 * @param page Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
 * @throws HashNotFoundException Thrown if the page is not in the buffer pool.
 *
 */
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{   
    //We want to first check if this page is already in the buffer pool
    FrameId fId;
    try 
    {
        hashTable->lookup(file, pageNo, fId);
    }

    //Case 1: The page does not exist in the buffer pool
    catch (const HashNotFoundException& e) 
    {
    //Call allocBuf() to allocate a buffer frame
    FrameId returnValue;
    allocBuf(returnValue);
    //Call the method file->readPage() to read the page from disk into the buffer pool frame
    bufPool[returnValue] = file->readPage(pageNo);
    //Insert the page into the hashtable
    hashTable->insert(file, pageNo, returnValue);
    //Invoke Set() on the frame to set it up properly
    bufDescTable[returnValue].Set(file, pageNo);
    //Return a pointer to the frame containing the page via the page parameter
    page = &(bufPool[returnValue]);
    return; 
    }

    //Case 2: The page exists in the buffer pool
    //Set the appropriate refbit
    bufDescTable[fId].refbit = true;
    //Increment the pinCnt for the page
    bufDescTable[fId].pinCnt++;
    //Return a pointer to the frame containing the page via the page parameter
    page = &(bufPool[fId]); // the "return" is here
    return;
}

/**
 * Unpin a page from memory since it is no longer required for it to remain in memory.
 *
 * @param file   	File object.
 * @param PageNo  Page number.
 * @param dirty		True if the page to be unpinned needs to be marked dirty.
 * @throws  HashNotFoundException Thrown If the page is not already pinned.
 * @throws  PageNotPinnedException Thrown If the page is not already pinned.
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo = 0;

	try{
		//Check if our file and pageNo is in the buffer pool, and if not, throw HashNotFoundException and return (catch block)
		hashTable->lookup(file, pageNo, frameNo);

		//If pincount is set to 0, throw PageNotPinnedException
		if(bufDescTable[frameNo].pinCnt == 0)
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		
		//else, decrement pin count
		--bufDescTable[frameNo].pinCnt;
		
		//if page is dirty, mark as dirty
		if(dirty)
			bufDescTable[frameNo].dirty = true;
	}

	//returns after catching our HashNotFoundException
	catch(HashNotFoundException& e){
		return;
	}
}

/**
 * Allocates a new, empty page in the file and returns the Page object.
 * The newly allocated page is also assigned a frame in the buffer pool.
 *
 * @param file   	File object
 * @param PageNo  	Page number. The number assigned to the page in the file is returned via this reference.
 * @param page  	Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	//obtain a buffer pool frame by calling allocBuff
	FrameId fId;
	allocBuf(fId);
	
	//allocate an empty page in the specified file
	Page p = file->allocatePage();
	pageNo = p.page_number();
	
	//Insert entry into hashtable, and invoke Set(), and sets pointer to 
	//the buffer frame via the page parameter  
	hashTable->insert(file, pageNo, fId);
	bufDescTable[fId].Set(file, pageNo);
	bufPool[fId] = p;
	page = &(bufPool[fId]);
	return;
}

/**
* Writes out all dirty pages of the file to disk.
* All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called,
* Otherwise Error returned.
*
* @param file   	File object
* @throws  PagePinnedException If any page of the file is pinned in the buffer pool 
* @throws BadBufferException If any frame allocated to the file is found to be invalid
*/
void BufMgr::flushFile(const File* file) 
{
	for(FrameId i = 0; i < numBufs; ++i){
		if(bufDescTable[i].file == file){
			try{
				// Write dirty page and check page is valid or not
				if(bufDescTable[i].dirty){
					bufDescTable[i].file->writePage(bufPool[i]); // If page is invalid, it will throw InvalidPageException
					bufDescTable[i].dirty = false;
				}

				// If file is still pinned, throw exception
				if(bufDescTable[i].pinCnt){
					throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, i);
				}

				// remove hash table entry
				hashTable->remove(file, bufDescTable[i].pageNo);

				// Clear() the bufDescTable[i]
				bufDescTable[i].Clear();
			}
			//catch invalid page exception, to throw a BadBufferException  
			catch(const InvalidPageException& e){
				throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			}
		}
	}
}
/**
* Delete page from file and also from buffer pool if present.
* Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
*
* @param file   	File object
* @param PageNo  Page number
*/
void BufMgr::disposePage(File* file, const PageId PageNo)
{
	// Note: This function does not check whether the pinCnt is already 0!
	
	// Find if the page exists in buffer
	FrameId fId;
	try{
		hashTable->lookup(file, PageNo, fId);
		// Continue means lookup succeeded. Otherwise HashNotFoundException is thrown and execution flow goes to catch(){}.
		
		// Remove entries in bufDescTable, hashTable
		bufDescTable[fId].Clear();
		hashTable->remove(file, PageNo);

		// We left bufPool[i] data in place, which may be a security issue.
		// Now we can dispose page on disk.
	}
	catch(const HashNotFoundException& e){
		// Lookup failed. We can move on disposing page on disk.
	}

	// Dispose page on disk
	file->deletePage(PageNo);
}

/**
* Print member variable values. 
*/
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