pub struct CircularBuffer<T> {
    storage: Vec<Option<T>>,
    head: usize,
    tail: usize,
}

impl<T> CircularBuffer<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be greater than zero");

        let storage = (0..=capacity).map(|_| None).collect();

        Self {
            storage,
            head: 0,
            tail: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.storage.len() - 1
    }

    pub fn len(&self) -> usize {
        if self.head >= self.tail {
            self.head - self.tail
        } else {
            self.storage.len() - (self.tail - self.head)
        }
    }

    pub fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    pub fn is_full(&self) -> bool {
        self.increment(self.head) == self.tail
    }

    pub fn push(&mut self, value: T) -> Result<(), T> {
        if self.is_full() {
            return Err(value);
        }

        self.storage[self.head] = Some(value);
        self.head = self.increment(self.head);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }

        let value = self.storage[self.tail].take();
        self.tail = self.increment(self.tail);
        value
    }

    pub fn clear(&mut self) {
        while self.pop().is_some() {}
    }

    fn increment(&self, index: usize) -> usize {
        (index + 1) % self.storage.len()
    }
}
