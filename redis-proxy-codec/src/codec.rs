use std::ops::Range;

use btoi::btoi;
use bytes::{Bytes, BytesMut};
use memchr::memchr;
use thiserror::Error;
use tokio_util::codec::Decoder;

#[derive(Debug, Error, PartialEq)]
pub enum ParseError {
    #[error("invalid protocol")]
    InvalidProtocol,
    #[error("not enough data")]
    NotEnoughData,
    #[error("unexpected error")]
    UnexpectedErr,
    #[error("io error")]
    IOError,
}

impl From<std::io::Error> for ParseError {
    fn from(e: std::io::Error) -> Self {
        ParseError::IOError
    }
}

pub struct MyClientCodec {}

impl MyClientCodec {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Debug, Clone)]
pub struct IndexedResp {
    pub resp: RespIndex,
    pub data: Bytes,
}


impl IndexedResp {
    pub fn get_array_element(&self, index: usize) -> Option<&[u8]> {
        match self.resp {
            RespIndex::Arr(ref resps) => {
                resps.0.get(index).and_then(|resp| match resp {
                    RespIndex::Bulk(s) => Some(
                        self.data
                            .get(s.to_range())
                            .expect("IndexedResp::get_array_element"),
                    ),
                    _ => None,
                })
            }
            _ => None,
        }
    }
    pub fn get_command_name(&self) -> Option<&str> {
        let element = self.get_array_element(0)?;
        std::str::from_utf8(element).ok()
    }
    pub fn get_array_len(&self) -> Option<usize> {
        match self.resp {
            RespIndex::Arr(ref resps) => Some(resps.0.len()),
            _ => None,
        }
    }
}


impl Decoder for MyClientCodec {
    type Item = IndexedResp;
    type Error = ParseError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let result = parse_resp(&src)?;
        if result.is_none() {
            return Ok(None);
        }
        let (mut arr, consumed) = result.unwrap();
        println!("arr: {:?}, consumed: {}", arr, consumed);
        let src = src.split_to(consumed);
        return Ok(Some(IndexedResp { resp: arr, data: src.freeze() }));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataIndex(pub usize, pub usize);

pub type RespIndex = Resp<DataIndex>;

impl AdvanceIndex for DataIndex {
    fn advance(&mut self, count: usize) {
        self.0 += count;
        self.1 += count;
    }
}

impl DataIndex {
    pub fn to_range(&self) -> Range<usize> {
        self.0..self.1
    }
}

pub const LF: u8 = b'\n';

fn parse_line(buf: &[u8]) -> Result<(DataIndex, usize), ParseError> {
    let lf_index = memchr(LF, buf).ok_or(ParseError::NotEnoughData)?;
    if lf_index == 0 {
        return Err(ParseError::InvalidProtocol);
    }

    // s >= 2
    // Just ignore the CR
    let line = DataIndex(0, lf_index + 1 - 2);
    Ok((line, lf_index + 1))
}

pub fn parse_resp(buf: &[u8]) -> Result<Option<(RespIndex, usize)>, ParseError> {
    if buf.is_empty() {
        return Ok(None);
    }

    let prefix = *buf.first().ok_or(ParseError::UnexpectedErr)?;
    let next_buf = buf.get(1..).ok_or(ParseError::InvalidProtocol)?;

    match prefix {
        b'*' => {
            let result = parse_array(next_buf)?;
            if result.is_none() {
                return Ok(None);
            }
            let (mut v, consumed) = result.unwrap();
            v.advance(1);
            Ok(Some((RespIndex::Arr(v), 1 + consumed)))
        }
        b'$' => {
            let (mut v, consumed) = parse_bulk_str(next_buf)?;
            v.advance(1);
            Ok(Some((RespIndex::Bulk(v), 1 + consumed)))
        }
        _ => {
            Err(ParseError::InvalidProtocol)
        }
    }
}


pub type ArrayIndex = Array<DataIndex>;


pub trait AdvanceIndex {
    fn advance(&mut self, count: usize);
}

impl<T> AdvanceIndex for Array<T> where T: AdvanceIndex {
    fn advance(&mut self, count: usize) {
        for x in &mut self.0 {
            x.advance(count);
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Array<T>(pub Vec<Resp<T>>);


#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Resp<T> {
    Error(T),
    Simple(T),
    Bulk(T),
    Integer(T),
    Arr(Array<T>),
}

impl<T> AdvanceIndex for Resp<T> where T: AdvanceIndex {
    fn advance(&mut self, count: usize) {
        match self {
            Resp::Error(_) => {}
            Resp::Simple(_) => {}
            Resp::Bulk(bulk) => {
                bulk.advance(count);
            }
            Resp::Integer(_) => {}
            Resp::Arr(arr) => {
                arr.advance(count);
            }
        }
    }
}

fn parse_array(buf: &[u8]) -> Result<Option<(ArrayIndex, usize)>, ParseError> {
    let (len, mut consumed) = parse_len(buf)?;

    if len < 0 {
        return Err(ParseError::InvalidProtocol);
    }

    let array_size = len as usize;
    let mut array = Vec::with_capacity(array_size);

    for _ in 0..array_size {
        let next_buf = buf.get(consumed..).ok_or(ParseError::InvalidProtocol)?;
        let res = parse_resp(next_buf)?;
        if res.is_none() {
            return Ok(None);
        }
        let (mut v, element_consumed) = res.unwrap();
        v.advance(consumed);
        consumed += element_consumed;
        array.push(v);
    }

    Ok(Some((Array(array), consumed)))
}

fn parse_len(buf: &[u8]) -> Result<(i64, usize), ParseError> {
    let (data_index, consumed) = parse_line(buf)?;
    let next_buf = buf
        .get(data_index.to_range())
        .ok_or(ParseError::UnexpectedErr)?;

    let len = btoi(next_buf).map_err(|_| ParseError::InvalidProtocol)?;
    Ok((len, consumed))
}

fn parse_bulk_str(buf: &[u8]) -> Result<(DataIndex, usize), ParseError> {
    let (len, consumed) = parse_len(buf)?;
    if len < 0 {
        return Err(ParseError::InvalidProtocol);
    }

    let content_size = len as usize;
    if buf.len() < consumed + content_size + 2 {
        return Err(ParseError::NotEnoughData);
    }

    let s = DataIndex(consumed, consumed + content_size);
    Ok((s, consumed + content_size + 2))
}

