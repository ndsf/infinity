/*
 * Memory - Buffer
 *
 * (c) 2018 Claude Barthels, ETH Zurich
 * Contact: claudeb@inf.ethz.ch
 *
 */

#include "Buffer.h"

#include <stdlib.h>
#include <string.h>

#include <infinity/core/Configuration.h>
#include <infinity/utils/Debug.h>

#define MIN(a,b) (((a)<(b)) ? (a) : (b))

namespace infinity {
namespace memory {

Buffer::Buffer(infinity::core::Context* context, uint64_t sizeInBytes, std::string path, std::string layout) {

	this->context = context;
	// this->rootp->len = sizeInBytes;TEST();
	// this->sizeInBytes = sizeInBytes;
	this->memoryRegionType = RegionType::BUFFER;

	// Create the pmemobj pool or open it if it already exists
	this->pop = pmemobj_create(path.c_str(), layout.c_str(), PMEMOBJ_MIN_POOL, 0666); // res->pop = pmemobj_open(FILE_NAME, LAYOUT_NAME); // FAILED
	// Check if create failed		
	if (this->pop == NULL) 
	{
		if (errno == 17)
		{
			printf("File exists, trying to open %s\n", path);
			pop = pmemobj_open(path.c_str(), layout.c_str());
			if (pop == NULL)
			{
				printf("Errored %s\n", strerror(errno));
				exit(1);
			}
		}
		else
		{
			printf("Errored %s\n", strerror(errno));
			exit(1);
		}
	}

	this->root = pmemobj_root(this->pop, sizeof(my_root));
	this->rootp = reinterpret_cast<infinity::memory::my_root *>(pmemobj_direct(this->root));
	this->rootp->len = sizeInBytes;
	// int res = posix_memalign(&(this->data), infinity::core::Configuration::PAGE_SIZE, sizeInBytes);
	// INFINITY_ASSERT(res == 0, "[INFINITY][MEMORY][BUFFER] Cannot allocate and align buffer.\n");

	// memset(this->data, 0, sizeInBytes);

	this->ibvMemoryRegion = ibv_reg_mr(this->context->getProtectionDomain(), this->rootp->buf, this->rootp->len,
			IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
	INFINITY_ASSERT(this->ibvMemoryRegion != NULL, "[INFINITY][MEMORY][BUFFER] Registration failed.\n");

	this->memoryAllocated = true;
	this->memoryRegistered = true;

}

Buffer::Buffer(infinity::core::Context* context, infinity::memory::RegisteredMemory* memory, uint64_t offset, uint64_t sizeInBytes) {

	this->context = context;
	this->sizeInBytes = sizeInBytes;
	this->memoryRegionType = RegionType::BUFFER;

	this->data = reinterpret_cast<char *>(memory->getData()) + offset;
	this->ibvMemoryRegion = memory->getRegion();

	this->memoryAllocated = false;
	this->memoryRegistered = false;

}

Buffer::Buffer(infinity::core::Context *context, void *memory, uint64_t sizeInBytes, std::string path, std::string layout) { // ADD PMEM PERSIST

	this->context = context;
	// this->sizeInBytes = sizeInBytes;
	this->memoryRegionType = RegionType::BUFFER;

	// Create the pmemobj pool or open it if it already exists
	this->pop = pmemobj_create(path.c_str(), layout.c_str(), PMEMOBJ_MIN_POOL, 0666); // res->pop = pmemobj_open(FILE_NAME, LAYOUT_NAME); // FAILED
	// Check if create failed		
	if (this->pop == NULL) 
	{
		if (errno == 17)
		{
			printf("File exists, trying to open %s\n", path);
			pop = pmemobj_open(path.c_str(), layout.c_str());
			if (pop == NULL)
			{
				printf("Errored %s\n", strerror(errno));
				exit(1);
			}
		}
		else
		{
			printf("Errored %s\n", strerror(errno));
			exit(1);
		}
	}

	this->root = pmemobj_root(this->pop, sizeof(my_root));
	this->rootp = reinterpret_cast<infinity::memory::my_root *>(pmemobj_direct(this->root));

	this->rootp->len = sizeInBytes;
	pmemobj_persist(this->pop, &this->rootp->len, sizeof(this->rootp->len));
	pmemobj_memcpy_persist(this->pop, this->rootp->buf, memory, this->rootp->len);

	// this->data = memory;
	this->ibvMemoryRegion = ibv_reg_mr(this->context->getProtectionDomain(), this->rootp->buf, this->rootp->len,
			IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
	INFINITY_ASSERT(this->ibvMemoryRegion != NULL, "[INFINITY][MEMORY][BUFFER] Registration failed.\n");

	this->memoryAllocated = false;
	this->memoryRegistered = true;

}

Buffer::~Buffer() {

	if (this->memoryRegistered) {
		ibv_dereg_mr(this->ibvMemoryRegion);
	}
	if (this->memoryAllocated) {
		// free(this->data);
		pmemobj_close(this->pop);
	}

}

void* Buffer::getData() {
	return reinterpret_cast<void *>(this->getAddress());
}

void Buffer::resize(uint64_t newSize, void* newData) {

	void *oldData = this->data;
	uint32_t oldSize = this->sizeInBytes;

	if (newData == NULL) {
		newData = this->data;
	}

	if (oldData != newData) {
		uint64_t copySize = MIN(newSize, oldSize);
		memcpy(newData, oldData, copySize);
	}

	if (memoryRegistered) {
		ibv_dereg_mr(this->ibvMemoryRegion);
		this->ibvMemoryRegion = ibv_reg_mr(this->context->getProtectionDomain(), newData, newSize,
				IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
		this->data = newData;
		this->sizeInBytes = newSize;
	} else {
		INFINITY_ASSERT(false, "[INFINITY][MEMORY][BUFFER] You can only resize memory which has registered by this buffer.\n");
	}
}

} /* namespace memory */
} /* namespace infinity */
