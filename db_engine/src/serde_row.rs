use allocator_api2::alloc::Allocator;
use crate::types::{PageBufferReader};
use allocator_api2::vec::Vec;
use crate::table::{Table, ColumnType, Flags};
use crate::db::{InsertErr};

#[derive(Debug)]
pub enum Value<'a> {
    Null,
    Int(i64),
    Float(f64),
    Chars(&'a [u8])
}

pub trait Operations<'a> {
    fn eq(&self, rhs: &Value<'a>) -> bool;
    fn gt(&self, rhs: &Value<'a>) -> bool;
    fn lt(&self, rhs: &Value<'a>) -> bool;
    fn starts_with(&self, rhs: &Value<'a>) -> bool;
    fn ends_with(&self, rhs: &Value<'a>) -> bool;
    fn contains(&self, rhs: &Value<'a>) -> bool;
    fn is_null(&self) -> bool;
}

impl <'a> Value<'a> {
    pub fn to_int(&self) -> Option<i64> {
        match self {
            Value::Int(val) => Some(*val),
            _ => None
        }
    }

    pub fn to_chars(&self) -> Option<&'a [u8]> {
        match self {
            Value::Chars(val) => Some(val),
            _ => None
        }
    }
}

impl<'a> Operations<'a> for Value<'a> {
    fn eq(&self, rhs: &Value<'a>) -> bool {
        match (self, rhs) {
            (Value::Null, Value::Null) => true,
            (Value::Int(l), Value::Int(r)) => l == r,
            (Value::Float(l), Value::Float(r)) => l == r,
            (Value::Chars(l), Value::Chars(r)) => l == r,
            _ => false,
        }
    }

    fn gt(&self, rhs: &Value<'a>) -> bool {
        matches!((self, rhs), (Value::Int(l), Value::Int(r)) if l > r) ||
        matches!((self, rhs), (Value::Float(l), Value::Float(r)) if l > r) ||
        matches!((self, rhs), (Value::Chars(l), Value::Chars(r)) if l > r)
    }

    fn lt(&self, rhs: &Value<'a>) -> bool {
        matches!((self, rhs), (Value::Int(l), Value::Int(r)) if l < r) ||
        matches!((self, rhs), (Value::Float(l), Value::Float(r)) if l < r) ||
        matches!((self, rhs), (Value::Chars(l), Value::Chars(r)) if l < r)
    }

    fn starts_with(&self, rhs: &Value<'a>) -> bool {
        matches!((self, rhs), (Value::Chars(l), Value::Chars(r)) if l.starts_with(r))
    }

    fn ends_with(&self, rhs: &Value<'a>) -> bool {
        matches!((self, rhs), (Value::Chars(l), Value::Chars(r)) if l.ends_with(r))
    }
    
    fn contains(&self, rhs: &Value<'a>) -> bool {
        matches!((self, rhs), (Value::Chars(l), Value::Chars(r)) if l.windows(r.len()).any(|w| w == *r))
    }

    fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

pub const CHARS_MAX_LEN: usize = 255;

pub type Row<'a, A> = allocator_api2::vec::Vec<Value<'a>, A>;

impl<'a> Value<'a> {
    pub fn to_bytes_vec<A: Allocator + Clone>(&'a self, v: &mut Vec<u8, A>) {
        match self {
            Value::Null => {},
            Value::Int(val) => v.extend_from_slice(&val.to_le_bytes()),
            Value::Float(val) => v.extend_from_slice(&val.to_le_bytes()),
            Value::Chars(val) => {
                let limit = val.len().min(CHARS_MAX_LEN);
                v.extend_from_slice(&val[..limit]);
            }
        }
    }
}

#[derive(Debug)]
pub struct SerializedRow<A: Allocator + Clone> {
    pub key: Vec<u8, A>,
    pub null_flags: Vec<u8, A>,
    pub payload: Vec<u8, A>,
}

pub fn serialize<A: Allocator + Clone>(table: &Table, row: &Row<A>, allocator: A) -> Result<SerializedRow<A>, InsertErr> {
    let mut payload: Vec<u8, A> = Vec::new_in(allocator.clone());
    let mut null_flags: u64 = 0;
    let mut i = 0;
    let mut key: Vec<u8, A> = Vec::new_in(allocator.clone());

    while i < row.len() {
        let col = &table.columns[i];
        match &row[i] {
            Value::Null => {
                if Flags::is_set(col.flags, Flags::Primary) || !Flags::is_set(col.flags, Flags::Nullable) {
                    return Err(InsertErr::CannotBeNull);
                }
                if Flags::is_set(col.flags, Flags::Ref) {
                    todo!("ref key check");
                }
                null_flags |= 1 << i;
            },
            Value::Int(val) => {
                if col.col_type != ColumnType::Int {
                    return Err(InsertErr::TypeDoesNotMatch);
                }
                if Flags::is_set(col.flags, Flags::Ref) {
                    todo!("ref key check");
                }
                payload.extend_from_slice(&val.to_le_bytes()); 
            },
            Value::Float(val) => {
                if col.col_type != ColumnType::Float {
                    return Err(InsertErr::TypeDoesNotMatch);
                }
                if Flags::is_set(col.flags, Flags::Ref) {
                    todo!("ref key check");
                }
                payload.extend_from_slice(&val.to_le_bytes());
            },
            Value::Chars(val) => {
                if col.col_type != ColumnType::Chars {
                    return Err(InsertErr::TypeDoesNotMatch);
                }
                if Flags::is_set(col.flags, Flags::Ref) {
                    todo!("ref key check");
                }
                let length = val.len() as u8; 
                payload.push(length);
                payload.extend_from_slice(val);
            }

        }

        if Flags::is_set(table.columns[i].flags, Flags::Primary) {
            row[i].to_bytes_vec(&mut key);
        }

        i += 1;
    }

    let num_cols = table.col_count;
    let num_bytes = ((num_cols + 7) / 8) as usize;
    let all_bytes = null_flags.to_le_bytes();
    let mut null_flags = Vec::with_capacity_in(num_bytes, allocator.clone());
    null_flags.extend_from_slice(&all_bytes[..num_bytes]);

    Ok(SerializedRow {
        key: key,
        null_flags: null_flags,
        payload: payload,
    })
}

pub fn deserialize<'a, A: Allocator + Clone>(
    table: &'a Table,
    row: &mut Row<'a, A>,
    payload: &'a mut Vec<u8, A>,
) {
    let mut reader = PageBufferReader::new(payload.as_slice());
    let mut i = 0;
    while i < table.col_count as usize {
        let col = &table.columns[i];

        match col.col_type {
            ColumnType::Int => {
                let val: i64 = reader.read(); 
                row.push(Value::Int(val));
            },
            ColumnType::Float => {
                let val: f64 = reader.read(); 
                row.push(Value::Float(val));
            },
            ColumnType::Chars => {
                let len: u8 = reader.read(); 
                let chars = reader.read_slice(len as usize);
                row.push(Value::Chars(chars));
            },
            _ => {}
        }

        i += 1;
    }
}
