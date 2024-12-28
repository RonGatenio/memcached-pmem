/*
 * Copyright 2018 Lenovo
 *
 * Licensed under the BSD-3 license. see LICENSE.Lenovo.txt for full text
 */
#ifndef PSLAB_H
#define PSLAB_H

#include <libpmem.h>
#include <emmintrin.h>

#define	PSLAB_POLICY_DRAM 0
#define	PSLAB_POLICY_PMEM 1
#define	PSLAB_POLICY_BALANCED 2

inline void _persist(char* data, size_t len)
{
    #define kCacheLineSize (64)

    volatile char *ptr = (char*)((unsigned long)data & (~(kCacheLineSize-1)));
    _mm_sfence();
    for (; ptr < data+len; ptr+=kCacheLineSize) {
        _mm_clflush((const void*)ptr);
    }
    _mm_sfence();
}

#define PERSIST(func, addr, size) do { func((addr), (size)); _persist((addr), (size)); } while(false)

#define pmem_member_persist(p, m) \
    PERSIST(pmem_persist, &(p)->m, sizeof ((p)->m))
#define pmem_member_flush(p, m) \
    PERSIST(pmem_flush, &(p)->m, sizeof ((p)->m))
#define pmem_flush_from(p, t, m) \
    PERSIST(pmem_flush, &(p)->m, sizeof (t) - offsetof(t, m))
#define pslab_item_data_persist(it) PERSIST(pmem_persist, (it)->data, ITEM_dtotal(it))
#define pslab_item_data_flush(it) PERSIST(pmem_flush, (it)->data, ITEM_dtotal(it))

int pslab_create(char *pool_name, uint32_t pool_size, uint32_t slab_size,
    uint32_t *slabclass_sizes, int slabclass_num);
int pslab_pre_recover(char *name, uint32_t *slab_sizes, int slab_max, int slab_page_size);
int pslab_do_recover(void);
time_t pslab_process_started(time_t process_started);
void pslab_update_flushtime(uint32_t time);
void pslab_use_slab(void *p, int id, unsigned int size);
void *pslab_get_free_slab(void *slab);
int pslab_contains(char *p);
uint64_t pslab_addr2off(void *addr);

extern bool pslab_force;

#endif
