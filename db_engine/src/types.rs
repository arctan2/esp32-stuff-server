use allocator_api2::boxed::Box;
use allocator_api2::alloc::Allocator;
use crate::page_rw::PAGE_SIZE;

pub struct PageBuffer<A: Allocator + Clone>(Box<[u8; PAGE_SIZE], A>);

impl <A> PageBuffer<A> where A: Allocator + Clone {
    pub fn new(allocator: A) -> Self {
        unsafe {
            Self(Box::new_zeroed_in(allocator).assume_init())
        }
    }

    pub unsafe fn write<T>(&mut self, offset: usize, val: &T) {
        unsafe {
            let bytes = core::slice::from_raw_parts(
                val as *const T as *const u8,
                core::mem::size_of::<T>()
            );
            self.0[offset..bytes.len()].copy_from_slice(bytes);
        }
    }

    pub unsafe fn read_mut_ref<T>(&mut self, offset: usize) -> T {
        unsafe {
            let size = core::mem::size_of::<T>();
            let ptr = self.0[offset..offset + size].as_mut_ptr() as *mut T;
            &mut *ptr
        }
    }

    pub unsafe fn read<T>(&mut self, offset: usize) -> T {
        unsafe {
            let size = core::mem::size_of::<T>();
            let src_slice = &self.0[offset..offset + size];
            core::ptr::read_unaligned(src_slice.as_ptr() as *const T)
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
