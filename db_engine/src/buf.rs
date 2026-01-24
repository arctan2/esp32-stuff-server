pub unsafe fn write<T>(buf: &mut [u8], offset: usize, val: &T) {
    unsafe {
        let bytes = core::slice::from_raw_parts(val as *const T as *const u8, size_of::<T>());
        buf[offset..offset + bytes.len()].copy_from_slice(bytes);
    }
}

pub unsafe fn write_bytes(buf: &mut [u8], offset: usize, bytes: &[u8]) {
    buf[offset..offset + bytes.len()].copy_from_slice(bytes);
}
