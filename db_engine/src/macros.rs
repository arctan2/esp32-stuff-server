#[macro_export]
macro_rules! set_bit {
    ($t:ty, $val:expr, $bit:expr) => { ($val as $t) |= (1 << ($bit as $t)) };
}

#[macro_export]
macro_rules! clear_bit {
    ($t:ty, $val:expr, $bit:expr) => { ($val as ty) &= !(1 << ($bit as ty)) };
}

#[macro_export]
macro_rules! get_bit {
    ($t:ty, $val:expr, $bit:expr) => { (($val as $t) >> ($bit as $t)) & 1 };
}

#[macro_export]
macro_rules! as_ref_mut {
    ($buf:expr, $T:ty) => {
        &mut *($buf.as_ptr_mut::<$T>(0)) as &mut $T
    }
}

#[macro_export]
macro_rules! as_ref {
    ($buf:expr, $T:ty) => {
        &*($buf.as_ptr::<$T>(0)) as &$T
    }
}
