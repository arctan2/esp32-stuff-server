#[macro_export]
macro_rules! set_bit {
    ($val:expr, $bit:expr) => { $val |= (1 << $bit) };
}

#[macro_export]
macro_rules! clear_bit {
    ($val:expr, $bit:expr) => { $val &= !(1 << $bit) };
}

#[macro_export]
macro_rules! get_bit {
    ($val:expr, $bit:expr) => { ($val >> $bit) & 1 };
}

