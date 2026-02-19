#![allow(unused)]
use core::ops::{Deref, DerefMut};
use tokio::sync::{Notify, Mutex as TokioMutex, MutexGuard as TokioMutexGuard};
use tokio::sync::mpsc::{Sender as TokioSender, Receiver as TokioReceiver};
use std::sync::Arc;

#[derive(Debug)]
pub struct Channel<T, const N: usize> {
    tx: TokioSender<T>,
    rx: TokioMutex<TokioReceiver<T>>,
}

#[derive(Debug)]
pub struct Sender<T, const N: usize> {
    s: TokioSender<T>,
}

impl <T, const N: usize> Clone for Sender<T, N> {
    fn clone(&self) -> Self {
        Self { s: self.s.clone() }
    }
}

impl<T, const N: usize> Sender<T, N> {
    pub async fn send(&self, msg: T) {
        self.s.send(msg).await.unwrap();
    }
}

#[derive(Debug)]
pub struct Receiver<T, const N: usize> {
    s: TokioMutex<TokioReceiver<T>>
}

impl<T, const N: usize> Receiver<T, N> {
    pub async fn recv(&self) -> T {
        self.s.lock().await.recv().await.unwrap()
    }
}

impl<T, const N: usize> Channel<T, N> {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(N);
        Self { tx, rx: TokioMutex::new(rx) }
    }

    pub fn sender(&self) -> Sender<T, N> {
        Sender { s: self.tx.clone() }
    }

    pub fn receiver(self) -> Receiver<T, N> {
        Receiver { s: self.rx }
    }

    pub async fn recv(&self) -> T {
        self.rx.lock().await.recv().await.unwrap()
    }

    pub async fn send(&self, msg: T) {
        self.tx.send(msg).await.unwrap();
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
