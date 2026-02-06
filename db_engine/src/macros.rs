#![allow(unused)]

use crate::page_free_list::PageFreeList;

#[macro_export]
macro_rules! get_bit {
    ($t:ty, $val:expr, $bit:expr) => {
        (($val as $t & $bit as $t) != 0) as u8
    };
}

#[macro_export]
macro_rules! set_bit {
    ($t:ty, $val:expr, $bit:expr) => {
        ($val as $t) | $bit as $t
    };
}

#[macro_export]
macro_rules! clear_bit {
    ($t:ty, $val:expr, $bit:expr) => {
        ($val as $t) & !($bit as $t)
    };
}

#[macro_export]
macro_rules! as_ref_mut {
    ($buf:expr, $T:ty) => {
        &mut *($buf.as_ptr_mut::<$T>(0)) as &mut $T
    };
    ($buf:expr, $T:ty, $off:expr) => {
        &mut *($buf.as_ptr_mut::<$T>($off)) as &mut $T
    }
}

#[macro_export]
macro_rules! as_ref {
    ($buf:expr, $T:ty) => {
        &*($buf.as_ptr::<$T>(0)) as &$T
    };
    ($buf:expr, $T:ty, $off:expr) => {
        &*($buf.as_ptr::<$T>($off)) as &$T
    }
}

#[macro_export]
macro_rules! get_free_page {
    ($page_rw:expr, $buf:expr) => {
        PageFreeList::get_free_page::<F, A>(
            $buf,
            $page_rw
        )
    };
}

#[macro_export]
macro_rules! add_page_to_free_list {
    ($page_rw:expr, $page_num:expr, $buf:expr) => {
        PageFreeList::add_page_to_list::<F, A>(
            $buf, 
            $page_num,
            $page_rw
        )
    };
}

