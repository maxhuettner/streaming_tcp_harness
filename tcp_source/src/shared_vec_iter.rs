use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct SharedVec<T> {
    data: Arc<Vec<T>>,
}

#[derive(Clone)]
pub struct SharedVecIterator<T> {
    data: Arc<Vec<T>>,
    position: Arc<AtomicUsize>,
}

impl<T: Clone> SharedVec<T> {
    pub fn new(data: Vec<T>) -> Self {
        Self {
            data: Arc::new(data),
        }
    }

    pub fn iter(&self) -> SharedVecIterator<T> {
        SharedVecIterator {
            data: Arc::clone(&self.data),
            position: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<T: Clone> Iterator for SharedVecIterator<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let position = self.position.fetch_add(1, Ordering::Relaxed);
        if position < self.data.len() {
            Some(self.data[position].clone())
        } else {
            None
        }
    }
}

impl<T: Clone> IntoIterator for &SharedVec<T> {
    type Item = T;
    type IntoIter = SharedVecIterator<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
