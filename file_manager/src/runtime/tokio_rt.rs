use core::ops::{Deref, DerefMut};
use tokio::sync::{mpsc, Notify, Mutex as TokioMutex, MutexGuard as TokioMutexGuard};
use std::sync::Arc;

#[derive(Debug)]
pub struct Channel<T, const N: usize> {
    tx: mpsc::Sender<T>,
    rx: tokio::sync::Mutex<mpsc::Receiver<T>>,
}

impl<T, const N: usize> Channel<T, N> {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(N);
        Self {
            tx,
            rx: tokio::sync::Mutex::new(rx),
        }
    }

    pub async fn send(&self, val: T) {
        let _ = self.tx.send(val).await;
    }

    pub async fn recv(&self) -> T {
        self.rx.lock().await.recv().await.unwrap()
    }
}

#[derive(Debug)]
pub struct Signal<T> {
    notify: Arc<Notify>,
    value: tokio::sync::Mutex<Option<T>>,
}

impl<T> Signal<T> {
    pub fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            value: tokio::sync::Mutex::new(None),
        }
    }

    pub async fn wait(&self) -> T {
        self.notify.notified().await;
        self.value.lock().await.take().unwrap()
    }

    pub async fn signal(&self, v: T) {
        *self.value.lock().await = Some(v);
        self.notify.notify_one();
    }

    pub fn reset(&self) {
    }
}

#[derive(Debug)]
pub struct Mutex<T> {
    m: TokioMutex<T>
}

#[derive(Debug)]
pub struct MutexGuard<'a, T> {
    g: TokioMutexGuard<'a, T>
}

impl <T> Mutex<T> {
    pub fn new(val: T) -> Self {
        Self { m: TokioMutex::new(val) }
    }

    pub async fn lock(&self) -> MutexGuard<'_, T> {
        MutexGuard { g: self.m.lock().await }
    }
}

impl<'a, T> Deref for MutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.g
    }
}

impl<'a, T> DerefMut for MutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.g
    }
}
