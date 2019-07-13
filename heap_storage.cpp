#pragma once

#include "db_cxx.h"
#include "storage_engine.h"
#include "heap_storage.h"


/**
* @class SlottedPage - heap file implementation of DbBlock.
*
*      Manage a database block that contains several records.
Modeled after slotted-page from Database Systems Concepts, 6ed, Figure 10-9.
Record id are handed out sequentially starting with 1 as records are added with add().
Each record has a header which is a fixed offset from the beginning of the block:
Bytes 0x00 - Ox01: number of records
Bytes 0x02 - 0x03: offset to end of free space
Bytes 0x04 - 0x05: size of record 1
Bytes 0x06 - 0x07: offset to record 1
etc.
*
*/

typedef u_int16_t u16;

//Constructor
//Paramaters: Block - page from the database that is using SlottedPage
//Paramaters: BlockID - id within DbFile
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
	if (is_new) {
		this->num_records = 0;
		this->end_free = DbBlock::BLOCK_SZ - 1;
		put_header();
	}
	else {
		get_header(this->num_records, this->end_free);
	}
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) throw(DbBlockNoRoomError) {
	if (!has_room(data->get_size()))
		throw DbBlockNoRoomError("not enough room for new record");
	u16 id = ++this->num_records;
	u16 size = (u16)data->get_size();
	this->end_free -= size;
	u16 loc = this->end_free + 1;
	put_header();
	put_header(id, size, loc);
	memcpy(this->address(loc), data->get_data(), size);
	return id;
}

//Get a record from the block. Return none if it has been deleted
Dbt* SlottedPage::get(RecordID record_id) {
	u_int16_t size;
	u_int16_t loc;

	get_header(size, loc, record_id);
	if (loc = 0) {
		return NULL;
	}

	Dbt* temp = new Dbt(this->address(loc), size);
	return temp;

}

//Replace the record with the given data. Raises ValueError if it won't fit
void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError) {
	u_int16_t size;
	u_int16_t loc;

	get_header(size, loc, record_id);

	u_int16_t new_size = data.get_size();

	if (new_size > size) {
		u_int16_t extra = new_size - size;
		if (!this->has_room(extra)) {
			throw DbBlockNoRoomError("not enough room for new record");
		}
		this->slide(loc, loc - extra);
		memcpy(this->address(loc - extra), data.get_data(), size);
	}
	else {
		memcpy(this->address(loc), data.get_data(), new_size);
		this->slide(loc + new_size, loc + size);
	}

	get_header(size, loc, record_id);
	put_header(record_id, new_size, loc);
}

//Mark the given record_id as deleted by changing its size to zero and its location to 0.
//Compact the rest of the data in the block. But keep the record ids the same for everyone.
void SlottedPage::del(RecordID record_id) {
	u_int16_t size;
	u_int16_t loc;

	get_header(size, loc, record_id);
	put_header(record_id, 0, 0);
	this->slide(loc, loc + size);
}

//Sequence of all non-deleted record ids.
RecordIDs* SlottedPage::ids(void) {
	u_int16_t size;
	u_int16_t loc;

	RecordIDs* temp = new RecordIDs();

	for (u_int16_t i = 0; i < this->num_records; i++) {
		if (get_header(size, loc, i) != 0) {
			temp.push_back(i);
		}
	}
	return temp;
}

//Get the size and offset for given record_id. For record_id of zero, it is the block header
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
	size = get_n(4 * id);
	loc = get_n(4 * id + 2);
}

//Put the size and offset for given record_id. For record_id of zero, store the block header
void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc) {
	if (id == 0) { // called the put_header() version and using the default params
		size = this->num_records;
		loc = this->end_free;
	}
	put_n(4 * id, size);
	put_n(4 * id + 2, loc);
}

//Calculate if we have room to store a record with given size. The size should include the
//4 bytes for the header, too, if this is an add
bool SlottedPage::has_room(u_int16_t size) {
	u_int16_t available;
	available = this->end_free - (this->num_records + 2) * 4;
	return (size <= available);
}

/**If start < end, then remove data from offset start up to but not including offset end by
*sliding data that is to the left of start to the right. If start > end, then make room for
*extra data from end to start by sliding data that is to the left of start to the left.
*Also fix up any record headers whose data has slid. Assumes there is enough room if it is
*left shift (end < start).
*/
void SlottedPage::slide(u_int16_t start, u_int16_t end) {
	u_int16_t shift;
	shift = end - start;
	if (shift == 0){
		return;
	}

	//slide data
	memcpy(this->address(this->end_free + 1 + shift), this->address(this->end_free + 1), shift);

	//fixup headers
	u_int16_t loc;
	u_int16_t size;
	RecordIDs* temp = new RecordIDs();

	for (unsigned int i = 0; i < temp->size(); i++) {
		get_header(size, loc, temp.at(i));
		if (loc <= start) {
			loc += shift;
			put_header(temp.at(i), size, loc);
		}
	}

	this->end_free += shift;
	this->put_header();
	delete temp;
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
	return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
	*(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
	return (void*)((char*)this->block.get_data() + offset);
}

/**************************Heap File Public Functions Implementation*********************/

/**
* @class HeapFile - Collection of blocks (implementation of DbFile)
*/

//Wrapper for Berkeley DB open, which does both open and creation
void HeapFile::db_open(uint16_t openflags) {
	if (!closed) {
		return;
	}
	Db data = new Db;
	data db = bdb.DB();
	this->db.set_re_len(DbBlock::BLOCK_SZ);
	this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0);
	//stat = db.stat(bdb.DB_FAST_STAT);
	//last = stat["ndata"]; 

	DB_BTREE_STAT *stat;
	this->db.stat(NULL, &stat, DB_FAST_STAT);
	this->last = stat->bt_ndata;
	this->closed = false;
}

//Create physical File
void HeapFile::create(void) {
	this->db_open(DB_CREATE | DB_EXCL); // "|": OR binary operator
	SlottedPage* block = this->get_new();
	delete block;
}

//Delete the physical file
void HeapFile::drop(void) {
	close();
	Db db(DB_ENV, 0);
	db.remove(this->dbfilename.c_str(), NULL, 0);
}

//Open physical file
void HeapFile::open(void) {
	db_open();
	// stat st;
	//BlockID block_size = stat("re_len");
}

//Close file
void HeapFile::close() {
	//this->write_lock = 1;
	db.close(0);
	closed = true;

}

//Get a block from the database file
SlottedPage* HeapFile::get(BlockID block_id) {
	/*while (write_queue) {
	if (block_id) {
	return write_queue(block_id);
	}
	}
	*/
	Dbt key(&block_id, sizeof(block_id));
	Dbt data;

	this->db.get(nullptr, &key, &data, 0);
	return SlottedPage(data, block_id, false);
}

//Returns the new empty DbBlock that is manging the records in this block and its block id
SlottedPage* HeapFile::get_new(void) {
	char block[DB_BLOCK_SZ];
	std::memset(block, 0, sizeof(block));
	Dbt data(block, sizeof(block));

	int block_id = ++this->last;
	Dbt key(&block_id, sizeof(block_id));

	// write out an empty block and read it back in so Berkeley DB is managing the memory
	SlottedPage* page = new SlottedPage(data, this->last, true);
	this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
	this->db.get(nullptr, &key, &data, 0);
	return page;
}

//Write a block back to the database file
void HeapFile::put(Db& block) {
	/*begin_write();
	this->write_queue(block_id) = block;
	end_write();
	*/
	BlockID block_id = block->get_block_id();
	Dbt key(&block_id, sizeof(block_id));
	this->db.put(NULL, &key, block->get_block(), 0);
}

//Sequence of all block ids
BlockIds* HeapFile::block_ids() {
	BlockIDs* id = new BlockIDs();
	for (BlockID i = 1; i <= this->(last); i++)
		id->push_back(i);
	return id;
}

/**************************Heap Table Public Functions Implementation*********************/

/**
* @class HeapTable - Heap storage engine (implementation of DbRelation)
*/
