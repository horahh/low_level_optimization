use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
pub struct AtomicOrderings {
    pub producer_tail_load: Ordering,
    pub producer_head_store: Ordering,
    pub consumer_head_load: Ordering,
    pub consumer_tail_store: Ordering,
}

impl AtomicOrderings {
    pub const STRICT: Self = Self {
        producer_tail_load: Ordering::Acquire,
        producer_head_store: Ordering::Release,
        consumer_head_load: Ordering::Acquire,
        consumer_tail_store: Ordering::Release,
    };

    pub const RELAXED: Self = Self {
        producer_tail_load: Ordering::Relaxed,
        producer_head_store: Ordering::Release,
        consumer_head_load: Ordering::Relaxed,
        consumer_tail_store: Ordering::Release,
    };
}

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
}

pub struct AtomicSpscCircularBuffer<T> {
    slots: Box<[Slot<T>]>,
    head: AtomicUsize,
    tail: AtomicUsize,
    orderings: AtomicOrderings,
}

impl<T> AtomicSpscCircularBuffer<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_orderings(capacity, AtomicOrderings::STRICT)
    }

    pub fn with_orderings(capacity: usize, orderings: AtomicOrderings) -> Self {
        assert!(capacity > 0, "capacity must be greater than zero");

        let slots = (0..=capacity)
            .map(|_| Slot {
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();

        Self {
            slots,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            orderings,
        }
    }

    pub fn capacity(&self) -> usize {
        self.slots.len() - 1
    }

    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        self.distance(head, tail)
    }

    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == self.tail.load(Ordering::Acquire)
    }

    pub fn is_full(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        self.increment(head) == tail
    }

    pub fn try_push(&self, value: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Relaxed);
        let next = self.increment(head);

        if next == self.tail.load(self.orderings.producer_tail_load) {
            return Err(value);
        }

        unsafe {
            (*self.slots[head].value.get()).write(value);
        }
        self.head.store(next, self.orderings.producer_head_store);
        Ok(())
    }

    pub fn try_pop(&self) -> Option<T> {
        let tail = self.tail.load(Ordering::Relaxed);

        if tail == self.head.load(self.orderings.consumer_head_load) {
            return None;
        }

        let value = unsafe { (*self.slots[tail].value.get()).assume_init_read() };
        self.tail
            .store(self.increment(tail), self.orderings.consumer_tail_store);
        Some(value)
    }

    fn increment(&self, index: usize) -> usize {
        (index + 1) % self.slots.len()
    }

    fn distance(&self, head: usize, tail: usize) -> usize {
        if head >= tail {
            head - tail
        } else {
            self.slots.len() - (tail - head)
        }
    }
}

impl<T> Drop for AtomicSpscCircularBuffer<T> {
    fn drop(&mut self) {
        let mut tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);

        while tail != head {
            unsafe {
                (*self.slots[tail].value.get()).assume_init_drop();
            }
            tail = (tail + 1) % self.slots.len();
        }
    }
}

unsafe impl<T: Send> Send for AtomicSpscCircularBuffer<T> {}
unsafe impl<T: Send> Sync for AtomicSpscCircularBuffer<T> {}
