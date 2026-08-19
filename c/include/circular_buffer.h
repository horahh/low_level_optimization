#ifndef LOW_LEVEL_OPTIMIZATION_CIRCULAR_BUFFER_H
#define LOW_LEVEL_OPTIMIZATION_CIRCULAR_BUFFER_H

#include <pthread.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stddef.h>

typedef struct {
    unsigned char *storage;
    size_t element_size;
    size_t storage_capacity;
    size_t head;
    size_t tail;
} cb_buffer_t;

typedef struct {
    cb_buffer_t inner;
    pthread_mutex_t mutex;
} cb_mutex_buffer_t;

typedef struct {
    unsigned char *storage;
    size_t element_size;
    size_t storage_capacity;
    _Atomic size_t head;
    _Atomic size_t tail;
} cb_atomic_spsc_t;

bool cb_init(cb_buffer_t *buffer, size_t capacity, size_t element_size);
void cb_free(cb_buffer_t *buffer);
size_t cb_capacity(const cb_buffer_t *buffer);
size_t cb_len(const cb_buffer_t *buffer);
bool cb_push(cb_buffer_t *buffer, const void *element);
bool cb_pop(cb_buffer_t *buffer, void *out_element);

bool cb_mutex_init(cb_mutex_buffer_t *buffer, size_t capacity, size_t element_size);
void cb_mutex_free(cb_mutex_buffer_t *buffer);
bool cb_mutex_push(cb_mutex_buffer_t *buffer, const void *element);
bool cb_mutex_pop(cb_mutex_buffer_t *buffer, void *out_element);

bool cb_atomic_spsc_init(
    cb_atomic_spsc_t *buffer,
    size_t capacity,
    size_t element_size
);
void cb_atomic_spsc_free(cb_atomic_spsc_t *buffer);
bool cb_atomic_spsc_push(cb_atomic_spsc_t *buffer, const void *element);
bool cb_atomic_spsc_pop(cb_atomic_spsc_t *buffer, void *out_element);
bool cb_atomic_spsc_push_explicit(
    cb_atomic_spsc_t *buffer,
    const void *element,
    memory_order producer_tail_load,
    memory_order producer_head_store
);
bool cb_atomic_spsc_pop_explicit(
    cb_atomic_spsc_t *buffer,
    void *out_element,
    memory_order consumer_head_load,
    memory_order consumer_tail_store
);

#endif
