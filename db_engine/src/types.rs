use allocator_api2::boxed::Box;
use allocator_api2::alloc::Allocator;
use crate::page_rw::PAGE_SIZE;
use core::mem::size_of;
use crate::buffer;

pub struct PageBuffer<A: Allocator + Clone>(pub Box<[u8; PAGE_SIZE], A>);

impl <A> PageBuffer<A> where A: Allocator + Clone {
    pub fn new(allocator: A) -> Self {
        unsafe {
            Self(Box::new_zeroed_in(allocator).assume_init())
        }
    }

    pub unsafe fn write<T>(&mut self, offset: usize, val: &T) {
        unsafe {
            buffer::write(&mut *self.0, offset, val);
        }
    }

    pub unsafe fn write_bytes(&mut self, offset: usize, bytes: &[u8]) {
        unsafe {
            buffer::write_bytes(&mut *self.0, offset, bytes);
        }
    }

    pub unsafe fn read<T>(&self, offset: usize) -> T {
        unsafe {
            let size = core::mem::size_of::<T>();
            let src_slice = &self.0[offset..offset + size];
            core::ptr::read_unaligned(src_slice.as_ptr() as *const T)
        }
    }

    pub unsafe fn as_ptr_mut<T>(&mut self, offset: usize) -> *mut T {
        let size = core::mem::size_of::<T>();
        let ptr = self.0[offset..offset + size].as_mut_ptr() as *mut T;
        ptr
    }

    pub unsafe fn as_ptr<T>(&self, offset: usize) -> *const T {
        let size = core::mem::size_of::<T>();
        let ptr = self.0[offset..offset + size].as_ptr() as *const T;
        ptr
    }

    pub unsafe fn as_type_mut<T>(&mut self, offset: usize) -> &mut T {
        unsafe {
            buffer::as_mut::<T>(&mut *self.0, offset)
        }
    }

    pub unsafe fn as_type_ref<T>(&self, offset: usize) -> &T {
        unsafe {
            buffer::as_ref::<T>(&*self.0, offset)
        }
    }
}

impl<A: Allocator + Clone> AsRef<[u8; PAGE_SIZE]> for PageBuffer<A> {
    fn as_ref(&self) -> &[u8; PAGE_SIZE] {
        &self.0
    }
}

impl<A: Allocator + Clone> AsMut<[u8; PAGE_SIZE]> for PageBuffer<A> {
    fn as_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.0
    }
}

pub struct PageBufferWriter<'a, A: Allocator + Clone> {
    buf: &'a mut PageBuffer<A>,
    pub cur_offset: usize,
}

impl <'a, A: Allocator + Clone> PageBufferWriter<'a, A> {
    pub fn new(buf: &'a mut PageBuffer<A>) -> Self {
        Self {
            buf: buf,
            cur_offset: 0
        }
    }

    pub fn write<T: core::fmt::Display>(&mut self, val: &T) {
        unsafe {
            self.buf.write(self.cur_offset, val);
        }
        self.cur_offset += size_of::<T>();
    }

    pub fn write_slice<S: AsRef<[u8]>>(&mut self, data: S) {
        let bytes = data.as_ref();
        unsafe {
            self.buf.write_bytes(self.cur_offset, bytes);
        }
        self.cur_offset += bytes.len();
    }
}
