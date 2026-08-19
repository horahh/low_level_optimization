use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use circular_buffer_rust::{
    AtomicOrderings, AtomicSpscCircularBuffer, CircularBuffer, MpmcCircularBuffer,
    MutexCircularBuffer,
};

#[test]
fn level1_wraps_and_uses_k_plus_one_storage() {
    let mut buffer = CircularBuffer::with_capacity(3);

    assert_eq!(buffer.capacity(), 3);
    assert!(buffer.is_empty());

    assert_eq!(buffer.push(1), Ok(()));
    assert_eq!(buffer.push(2), Ok(()));
    assert_eq!(buffer.push(3), Ok(()));
    assert!(buffer.is_full());
    assert_eq!(buffer.push(4), Err(4));

    assert_eq!(buffer.pop(), Some(1));
    assert_eq!(buffer.pop(), Some(2));
    assert_eq!(buffer.push(4), Ok(()));
    assert_eq!(buffer.push(5), Ok(()));

    assert_eq!(buffer.pop(), Some(3));
    assert_eq!(buffer.pop(), Some(4));
    assert_eq!(buffer.pop(), Some(5));
    assert_eq!(buffer.pop(), None);
}

#[test]
fn level2_mutex_supports_cross_thread_usage() {
    let buffer = Arc::new(MutexCircularBuffer::with_capacity(8));
    let producer_buffer = Arc::clone(&buffer);
    let consumer_buffer = Arc::clone(&buffer);

    let producer = thread::spawn(move || {
        for item in 0..128 {
            let mut value = item;
            loop {
                match producer_buffer.push(value) {
                    Ok(()) => break,
                    Err(returned) => {
                        value = returned;
                        thread::yield_now();
                    }
                }
            }
        }
    });

    let consumer = thread::spawn(move || {
        let mut values = Vec::new();
        while values.len() < 128 {
            if let Some(value) = consumer_buffer.pop() {
                values.push(value);
            } else {
                thread::yield_now();
            }
        }
        values
    });

    producer.join().unwrap();
    let values = consumer.join().unwrap();

    assert_eq!(values, (0..128).collect::<Vec<_>>());
}

#[test]
fn level3_spsc_moves_values_between_threads() {
    let buffer = Arc::new(AtomicSpscCircularBuffer::with_capacity(16));
    let producer_buffer = Arc::clone(&buffer);
    let consumer_buffer = Arc::clone(&buffer);

    let producer = thread::spawn(move || {
        for item in 0..256 {
            let mut value = item;
            loop {
                match producer_buffer.try_push(value) {
                    Ok(()) => break,
                    Err(returned) => {
                        value = returned;
                        thread::yield_now();
                    }
                }
            }
        }
    });

    let consumer = thread::spawn(move || {
        let mut values = Vec::new();
        while values.len() < 256 {
            if let Some(value) = consumer_buffer.try_pop() {
                values.push(value);
            } else {
                thread::yield_now();
            }
        }
        values
    });

    producer.join().unwrap();
    let values = consumer.join().unwrap();

    assert_eq!(values, (0..256).collect::<Vec<_>>());
}

#[test]
fn level3_supports_custom_orderings() {
    let buffer = AtomicSpscCircularBuffer::with_orderings(2, AtomicOrderings::RELAXED);

    assert_eq!(buffer.try_push(10), Ok(()));
    assert_eq!(buffer.try_push(20), Ok(()));
    assert_eq!(buffer.try_push(30), Err(30));

    assert_eq!(buffer.try_pop(), Some(10));
    assert_eq!(buffer.try_pop(), Some(20));
    assert_eq!(buffer.try_pop(), None);
}

#[test]
fn level4_mpmc_preserves_all_values() {
    let buffer = Arc::new(MpmcCircularBuffer::with_capacity(64));
    let consumed = Arc::new(AtomicUsize::new(0));
    let total = 240usize;

    let producer_a = {
        let buffer = Arc::clone(&buffer);
        thread::spawn(move || {
            for item in 0..120 {
                let mut value = item;
                loop {
                    match buffer.try_push(value) {
                        Ok(()) => break,
                        Err(returned) => {
                            value = returned;
                            thread::yield_now();
                        }
                    }
                }
            }
        })
    };

    let producer_b = {
        let buffer = Arc::clone(&buffer);
        thread::spawn(move || {
            for item in 120..240 {
                let mut value = item;
                loop {
                    match buffer.try_push(value) {
                        Ok(()) => break,
                        Err(returned) => {
                            value = returned;
                            thread::yield_now();
                        }
                    }
                }
            }
        })
    };

    let consumer_a = {
        let buffer = Arc::clone(&buffer);
        let consumed = Arc::clone(&consumed);
        thread::spawn(move || {
            let mut values = Vec::new();
            loop {
                if consumed.load(Ordering::Relaxed) >= total {
                    break;
                }

                if let Some(value) = buffer.try_pop() {
                    let previous = consumed.fetch_add(1, Ordering::Relaxed);
                    if previous < total {
                        values.push(value);
                    }
                    if previous + 1 >= total {
                        break;
                    }
                } else {
                    thread::yield_now();
                }
            }
            values
        })
    };

    let consumer_b = {
        let buffer = Arc::clone(&buffer);
        let consumed = Arc::clone(&consumed);
        thread::spawn(move || {
            let mut values = Vec::new();
            loop {
                if consumed.load(Ordering::Relaxed) >= total {
                    break;
                }

                if let Some(value) = buffer.try_pop() {
                    let previous = consumed.fetch_add(1, Ordering::Relaxed);
                    if previous < total {
                        values.push(value);
                    }
                    if previous + 1 >= total {
                        break;
                    }
                } else {
                    thread::yield_now();
                }
            }
            values
        })
    };

    producer_a.join().unwrap();
    producer_b.join().unwrap();

    let mut values = consumer_a.join().unwrap();
    values.extend(consumer_b.join().unwrap());
    values.sort_unstable();

    assert_eq!(values, (0..240).collect::<Vec<_>>());
}
