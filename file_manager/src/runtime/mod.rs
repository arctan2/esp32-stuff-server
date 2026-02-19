#![allow(unused)]
use core::ops::{Deref, DerefMut};

#[cfg(feature = "tokio")]
mod tokio_rt;

#[cfg(feature = "embassy")]
mod embassy_rt;
#[cfg(feature = "embassy")]
use embassy_rt::{
    Channel as ChannelInner,
    Signal as SignalInner,
    Mutex as MutexInner,
    MutexGuard as MutexGuardInner,
    Sender as SenderInner,
    Receiver as ReceiverInner
};

#[cfg(feature = "tokio")]
use tokio_rt::{
    Channel as ChannelInner,
    Signal as SignalInner,
    Mutex as MutexInner,
    MutexGuard as MutexGuardInner,
    Sender as SenderInner,
    Receiver as ReceiverInner
};

#[derive(Debug)]
pub struct Channel<T, const N: usize> {
    inner: ChannelInner<T, N>,
}

#[derive(Debug)]
pub struct Sender<T, const N: usize> {
    inner: SenderInner<T, N>,
}

impl <T, const N: usize> Clone for Sender<T, N> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<T, const N: usize> Sender<T, N> {
    pub async fn send(&self, msg: T) {
        self.inner.send(msg).await;
    }
}

#[derive(Debug)]
pub struct Receiver<T, const N: usize> {
    inner: ReceiverInner<T, N>,
}

impl<T, const N: usize> Receiver<T, N> {
    pub async fn recv(&self) -> T {
        self.inner.recv().await
    }
}

impl<T, const N: usize> Channel<T, N> {
    pub fn new() -> Self {
        Self { inner: ChannelInner::new() }
    }

    pub fn sender(&self) -> Sender<T, N> {
        Sender { inner: self.inner.sender() }
    }

    pub fn receiver(self) -> Receiver<T, N> {
        Receiver { inner: self.inner.receiver() }
    }

    pub async fn recv(&self) -> T {
        self.inner.recv().await
    }

    pub async fn send(&self, msg: T) {
        self.inner.send(msg).await;
    }
}

#[derive(Debug)]
pub struct Signal<T> {
    inner: SignalInner<T>,
}

impl<T> Signal<T> {
    pub fn new() -> Self {
        Self { inner: SignalInner::new() }
    }

    pub async fn wait(&self) -> T {
        self.inner.wait().await
    }

    pub async fn signal(&self, v: T) {
        self.inner.signal(v).await;
    }

    pub fn reset(&self) {
        self.inner.reset();
    }
}

#[derive(Debug)]
pub struct Mutex<T> {
    inner: MutexInner<T>
}

#[derive(Debug)]
pub struct MutexGuard<'a, T> {
    inner: MutexGuardInner<'a, T>
}

impl <T> Mutex<T> {
    pub fn new(val: T) -> Self {
        Self { inner: MutexInner::new(val) }
    }

    pub async fn lock(&self) -> MutexGuard<'_, T> {
        MutexGuard { inner: self.inner.lock().await }
    }
}

impl<'a, T> Deref for MutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, T> DerefMut for MutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
