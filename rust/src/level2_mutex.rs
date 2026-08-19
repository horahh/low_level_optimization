use std::sync::Mutex;

use crate::level1_simple::CircularBuffer;

pub struct MutexCircularBuffer<T> {
    inner: Mutex<CircularBuffer<T>>,
}

impl<T> MutexCircularBuffer<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(CircularBuffer::with_capacity(capacity)),
        }
    }

    pub fn capacity(&self) -> usize {
        self.with_lock(|buffer| buffer.capacity())
    }

    pub fn len(&self) -> usize {
        self.with_lock(|buffer| buffer.len())
    }

    pub fn is_empty(&self) -> bool {
        self.with_lock(|buffer| buffer.is_empty())
    }

    pub fn push(&self, value: T) -> Result<(), T> {
        self.with_lock(|buffer| buffer.push(value))
    }

    pub fn pop(&self) -> Option<T> {
        self.with_lock(|buffer| buffer.pop())
    }

    fn with_lock<R>(&self, f: impl FnOnce(&mut CircularBuffer<T>) -> R) -> R {
        let mut guard = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        f(&mut guard)
    }
}
