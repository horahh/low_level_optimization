use std::cell::UnsafeCell;
use std::hint::spin_loop;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

#[repr(align(64))]
struct CachePadded<T>(T);

struct Slot<T> {
    sequence: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

pub struct MpmcCircularBuffer<T> {
    slots: Box<[Slot<T>]>,
    mask: usize,
    enqueue_pos: CachePadded<AtomicUsize>,
    dequeue_pos: CachePadded<AtomicUsize>,
}

impl<T> MpmcCircularBuffer<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity >= 2, "capacity must be at least two");
        assert!(
            capacity.is_power_of_two(),
            "capacity must be a power of two"
        );

        let slots = (0..capacity)
            .map(|index| Slot {
                sequence: AtomicUsize::new(index),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();

        Self {
            slots,
            mask: capacity - 1,
            enqueue_pos: CachePadded(AtomicUsize::new(0)),
            dequeue_pos: CachePadded(AtomicUsize::new(0)),
        }
    }

    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    pub fn try_push(&self, value: T) -> Result<(), T> {
        let mut pos = self.enqueue_pos.0.load(Ordering::Relaxed);

        loop {
            let slot = &self.slots[pos & self.mask];
            let sequence = slot.sequence.load(Ordering::Acquire);
            let diff = sequence as isize - pos as isize;

            if diff == 0 {
                match self.enqueue_pos.0.compare_exchange_weak(
                    pos,
                    pos + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // The slot sequence number carries the synchronization for the data:
                        // a producer publishes with Release below, and consumers observe that
                        // publication with the Acquire load on `sequence` before reading.
                        unsafe {
                            (*slot.value.get()).write(value);
                        }
                        slot.sequence.store(pos + 1, Ordering::Release);
                        return Ok(());
                    }
                    Err(next_pos) => pos = next_pos,
                }
            } else if diff < 0 {
                return Err(value);
            } else {
                spin_loop();
                pos = self.enqueue_pos.0.load(Ordering::Relaxed);
            }
        }
    }

    pub fn try_pop(&self) -> Option<T> {
        let mut pos = self.dequeue_pos.0.load(Ordering::Relaxed);

        loop {
            let slot = &self.slots[pos & self.mask];
            let sequence = slot.sequence.load(Ordering::Acquire);
            let diff = sequence as isize - (pos as isize + 1);

            if diff == 0 {
                match self.dequeue_pos.0.compare_exchange_weak(
                    pos,
                    pos + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let value = unsafe { (*slot.value.get()).assume_init_read() };
                        slot.sequence
                            .store(pos + self.slots.len(), Ordering::Release);
                        return Some(value);
                    }
                    Err(next_pos) => pos = next_pos,
                }
            } else if diff < 0 {
                return None;
            } else {
                spin_loop();
                pos = self.dequeue_pos.0.load(Ordering::Relaxed);
            }
        }
    }
}

impl<T> Drop for MpmcCircularBuffer<T> {
    fn drop(&mut self) {
        let mut pos = self.dequeue_pos.0.load(Ordering::Relaxed);
        let end = self.enqueue_pos.0.load(Ordering::Relaxed);

        while pos != end {
            let slot = &mut self.slots[pos & self.mask];
            unsafe {
                (*slot.value.get()).assume_init_drop();
            }
            pos += 1;
        }
    }
}

unsafe impl<T: Send> Send for MpmcCircularBuffer<T> {}
unsafe impl<T: Send> Sync for MpmcCircularBuffer<T> {}
