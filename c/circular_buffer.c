#include "include/circular_buffer.h"

#include <stdlib.h>
#include <string.h>

static size_t cb_increment(size_t index, size_t storage_capacity) {
    return (index + 1U) % storage_capacity;
}

bool cb_init(cb_buffer_t *buffer, size_t capacity, size_t element_size) {
    if (buffer == NULL || capacity == 0U || element_size == 0U) {
        return false;
    }

    buffer->storage = calloc(capacity + 1U, element_size);
    if (buffer->storage == NULL) {
        return false;
    }

    buffer->element_size = element_size;
    buffer->storage_capacity = capacity + 1U;
    buffer->head = 0U;
    buffer->tail = 0U;
    return true;
}

void cb_free(cb_buffer_t *buffer) {
    if (buffer == NULL) {
        return;
    }

    free(buffer->storage);
    buffer->storage = NULL;
    buffer->element_size = 0U;
    buffer->storage_capacity = 0U;
    buffer->head = 0U;
    buffer->tail = 0U;
}

size_t cb_capacity(const cb_buffer_t *buffer) {
    return buffer->storage_capacity - 1U;
}

size_t cb_len(const cb_buffer_t *buffer) {
    if (buffer->head >= buffer->tail) {
        return buffer->head - buffer->tail;
    }

    return buffer->storage_capacity - (buffer->tail - buffer->head);
}

bool cb_push(cb_buffer_t *buffer, const void *element) {
    const size_t next = cb_increment(buffer->head, buffer->storage_capacity);
    if (next == buffer->tail) {
        return false;
    }

    memcpy(
        buffer->storage + (buffer->head * buffer->element_size),
        element,
        buffer->element_size
    );
    buffer->head = next;
    return true;
}

bool cb_pop(cb_buffer_t *buffer, void *out_element) {
    if (buffer->head == buffer->tail) {
        return false;
    }

    memcpy(
        out_element,
        buffer->storage + (buffer->tail * buffer->element_size),
        buffer->element_size
    );
    buffer->tail = cb_increment(buffer->tail, buffer->storage_capacity);
    return true;
}

bool cb_mutex_init(cb_mutex_buffer_t *buffer, size_t capacity, size_t element_size) {
    if (!cb_init(&buffer->inner, capacity, element_size)) {
        return false;
    }

    if (pthread_mutex_init(&buffer->mutex, NULL) != 0) {
        cb_free(&buffer->inner);
        return false;
    }

    return true;
}

void cb_mutex_free(cb_mutex_buffer_t *buffer) {
    if (buffer == NULL) {
        return;
    }

    pthread_mutex_destroy(&buffer->mutex);
    cb_free(&buffer->inner);
}

bool cb_mutex_push(cb_mutex_buffer_t *buffer, const void *element) {
    bool result;
    pthread_mutex_lock(&buffer->mutex);
    result = cb_push(&buffer->inner, element);
    pthread_mutex_unlock(&buffer->mutex);
    return result;
}

bool cb_mutex_pop(cb_mutex_buffer_t *buffer, void *out_element) {
    bool result;
    pthread_mutex_lock(&buffer->mutex);
    result = cb_pop(&buffer->inner, out_element);
    pthread_mutex_unlock(&buffer->mutex);
    return result;
}

bool cb_atomic_spsc_init(
    cb_atomic_spsc_t *buffer,
    size_t capacity,
    size_t element_size
) {
    if (buffer == NULL || capacity == 0U || element_size == 0U) {
        return false;
    }

    buffer->storage = calloc(capacity + 1U, element_size);
    if (buffer->storage == NULL) {
        return false;
    }

    buffer->element_size = element_size;
    buffer->storage_capacity = capacity + 1U;
    atomic_init(&buffer->head, 0U);
    atomic_init(&buffer->tail, 0U);
    return true;
}

void cb_atomic_spsc_free(cb_atomic_spsc_t *buffer) {
    if (buffer == NULL) {
        return;
    }

    free(buffer->storage);
    buffer->storage = NULL;
    buffer->element_size = 0U;
    buffer->storage_capacity = 0U;
    atomic_store_explicit(&buffer->head, 0U, memory_order_relaxed);
    atomic_store_explicit(&buffer->tail, 0U, memory_order_relaxed);
}

bool cb_atomic_spsc_push(cb_atomic_spsc_t *buffer, const void *element) {
    return cb_atomic_spsc_push_explicit(
        buffer,
        element,
        memory_order_acquire,
        memory_order_release
    );
}

bool cb_atomic_spsc_pop(cb_atomic_spsc_t *buffer, void *out_element) {
    return cb_atomic_spsc_pop_explicit(
        buffer,
        out_element,
        memory_order_acquire,
        memory_order_release
    );
}

bool cb_atomic_spsc_push_explicit(
    cb_atomic_spsc_t *buffer,
    const void *element,
    memory_order producer_tail_load,
    memory_order producer_head_store
) {
    const size_t head = atomic_load_explicit(&buffer->head, memory_order_relaxed);
    const size_t next = cb_increment(head, buffer->storage_capacity);
    if (next == atomic_load_explicit(&buffer->tail, producer_tail_load)) {
        return false;
    }

    memcpy(
        buffer->storage + (head * buffer->element_size),
        element,
        buffer->element_size
    );
    atomic_store_explicit(&buffer->head, next, producer_head_store);
    return true;
}

bool cb_atomic_spsc_pop_explicit(
    cb_atomic_spsc_t *buffer,
    void *out_element,
    memory_order consumer_head_load,
    memory_order consumer_tail_store
) {
    const size_t tail = atomic_load_explicit(&buffer->tail, memory_order_relaxed);
    if (tail == atomic_load_explicit(&buffer->head, consumer_head_load)) {
        return false;
    }

    memcpy(
        out_element,
        buffer->storage + (tail * buffer->element_size),
        buffer->element_size
    );
    atomic_store_explicit(
        &buffer->tail,
        cb_increment(tail, buffer->storage_capacity),
        consumer_tail_store
    );
    return true;
}
