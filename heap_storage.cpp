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

	get_header(&size, &loc, record_id);
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

	get_header(&size, &loc, record_id);

	u_int16_t new_size = data.length();

	if (new_size > size) {
		u_int16_t extra = new_size - size;
		if (!this->has_room(extra)) {
			throw DbBlockNoRoomError("not enough room for new record");
		}
		this->slide(loc, loc - extra);
		memcpy(this->address(loc - extra), data->get_data(), size);
	}
	else {
		memcpy(this->address(loc), data->get_data(), new_size);
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

	RecordID* temp = new RecordID();

	for (u_int16_t i = 0; i < this->num_records; i++) {
		if (get_header(size, loc, i) != 0) {
			temp.push_back(i);
		}
	}
	return temp;
}

/******************SlottedPage protected functions implementation*************************/

//Get the size and offset for given record_id. For record_id of zero, it is the block header
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
	size = get_n(4 * record_id);
	loc = get_n(4 * recird_id + 2);
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
	RecordID* temp = new RecordID();

	for (unsigned int i = 0; i < temp; i++) {
		get_header(size, loc, temp[i]);
		if (loc <= start) {
			loc += shift;
			put_header(temp[i], size, loc);
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

SlottedPage::~SlottedPage() {
	delete Dbt;
	delete RecordID();
}

/**************************Heap Table Public Functions Implementation*********************/

/**
* @class HeapTable - Heap storage engine (implementation of DbRelation)
*/
