pub unsafe fn write<T>(buf: &mut [u8], offset: usize, val: &T) {
    unsafe {
        let bytes = core::slice::from_raw_parts(val as *const T as *const u8, size_of::<T>());
        buf[offset..offset + bytes.len()].copy_from_slice(bytes);
    }
}

pub unsafe fn write_bytes(buf: &mut [u8], offset: usize, bytes: &[u8]) {
    buf[offset..offset + bytes.len()].copy_from_slice(bytes);
}

pub unsafe fn as_mut<T>(buf: &mut [u8], offset: usize) -> &mut T {
    unsafe {
        let size = core::mem::size_of::<T>();
        let ptr = &mut *(buf[offset..offset + size].as_mut_ptr() as *mut T) as &mut T;
        ptr
    }
}

pub unsafe fn as_ref<T>(buf: &[u8], offset: usize) -> T {
    unsafe {
        let size = core::mem::size_of::<T>();
        let ptr = buf[offset..offset + size].as_ptr() as *const T;
        core::ptr::read_unaligned(ptr)
    }
}

pub fn as_slice(buf: &[u8], offset: usize, len: usize) -> &[u8] {
    &buf[offset..offset + len]
}

pub fn copy_buf(dest: &mut [u8], src: &[u8]) {
    dest.copy_from_slice(src);
}
