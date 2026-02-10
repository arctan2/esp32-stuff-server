use core::ops::{Deref, DerefMut};

#[cfg(feature = "tokio")]
mod tokio_rt;

#[cfg(feature = "embassy")]
mod embassy_rt;

#[cfg(feature = "embassy")]
use embassy_rt::{Channel as ChannelInner, Signal as SignalInner, Mutex as MutexInner, MutexGuard as MutexGuardInner};

#[cfg(feature = "tokio")]
use tokio_rt::{Channel as ChannelInner, Signal as SignalInner, Mutex as MutexInner, MutexGuard as MutexGuardInner};

#[derive(Debug)]
pub struct Channel<T, const N: usize = 3> {
    inner: ChannelInner<T, N>,
}

impl<T, const N: usize> Channel<T, N> {
    pub fn new() -> Self {
        Self { inner: ChannelInner::new() }
    }

    pub async fn send(&self, v: T) {
        self.inner.send(v).await
    }

    pub async fn recv(&self) -> T {
        self.inner.recv().await
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
