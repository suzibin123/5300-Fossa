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

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) throw(DbBlockNoRoomError) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

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
	
	get_header(&size, &loc, record_id);
	put_header(record_id, new_size, loc);
}

void SlottedPage::del(RecordID record_id) {
	u_int16_t size;
	u_int16_t loc;

	get_header(&size, &loc, record_id);
	put_header(record_id, 0, 0);
	this->slide(loc, loc + size);
}

RecordIDs* SlottedPage::ids(void) {
	u_int16_t size;
	u_int16_t loc;

	RecordID* temp = new RecordID(); 

	return{
		for (u_int16_t i = 0; i < this->num_records; i++) {
			if (get_header(&size, &loc, i) != 0) {
				temp.push_back(i);
			}
		}
	}
}

SlottedPage::~SlottedPage() {
	delete Dbt; 
	delete RecordID(); 
}
