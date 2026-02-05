#[cfg(feature = "std")]
extern crate std;

use std::boxed::Box;
use std::vec;

use allocator_api2::alloc::{Allocator, AllocError, Layout};
use std::ptr::NonNull;
use buddy_system_allocator::LockedHeap;

#[derive(Clone)]
pub struct SimAllocator<const ORDER: usize>(pub &'static LockedHeap<ORDER>);

unsafe impl <const ORDER: usize> Allocator for SimAllocator<ORDER> {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let mut heap = self.0.lock();
        match heap.alloc(layout) {
            Ok(ptr) => {
                let slice = NonNull::slice_from_raw_parts(ptr, layout.size());
                Ok(slice)
            }
            Err(_) => Err(AllocError),
        }
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            self.0.lock().dealloc(ptr, layout);
        }
    }
}

impl<'a, const ORDER: usize> core::fmt::Debug for SimAllocator<ORDER> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // We use block scopes to copy the values out of the packed struct
        f.debug_struct("SimAllocator").finish()
    }
}

pub static INTERNAL_HEAP: LockedHeap<17> = LockedHeap::empty();
pub static PSRAM_HEAP: LockedHeap<23> = LockedHeap::empty();

pub fn init_simulated_hardware() {
    unsafe {
        let internal_ptr = Box::leak(vec![0u8; 64 * 1024].into_boxed_slice());
        let psram_ptr = Box::leak(vec![0u8; 4 * 1024 * 1024].into_boxed_slice());

        INTERNAL_HEAP.lock().init(
            internal_ptr.as_mut_ptr() as usize, 
            internal_ptr.len()
        );
        
        PSRAM_HEAP.lock().init(
            psram_ptr.as_mut_ptr() as usize, 
            psram_ptr.len()
        );
    }
}


