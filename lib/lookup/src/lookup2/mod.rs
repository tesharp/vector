mod jit;

use crate::lookup2::jit::{JitLookup, JitPath};
use serde::{Deserialize, Serialize};
use std::iter::Cloned;
use std::slice::Iter;

/// Use if you want to pre-parse paths so it can be used multiple times
/// The return value implements `Path` so it can be used directly
pub fn parse_path(path: &str) -> Vec<OwnedSegment> {
    JitPath::new(path)
        .segment_iter()
        .map(|segment| segment.into())
        .collect()
}

/// A path is simply the data describing how to look up a value
pub trait Path<'a> {
    type Iter: Iterator<Item = BorrowedSegment<'a>>;

    fn segment_iter(&self) -> Self::Iter;
}

impl<'a> Path<'a> for &'a Vec<OwnedSegment> {
    type Iter = OwnedSegmentSliceIter<'a>;

    fn segment_iter(&self) -> Self::Iter {
        OwnedSegmentSliceIter {
            segments: self.as_slice(),
            index: 0,
        }
        // self.as_slice().iter().map(OwnedSegment::borrow)
        // unimplemented!()
        // self.iter().cloned()
    }
}

pub struct OwnedSegmentSliceIter<'a> {
    segments: &'a [OwnedSegment],
    index: usize,
}

impl<'a> Iterator for OwnedSegmentSliceIter<'a> {
    type Item = BorrowedSegment<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let output = self.segments.get(self.index).map(|x| x.into());
        self.index += 1;
        output
    }
}

impl<'a, 'b: 'a> Path<'a> for &'b Vec<BorrowedSegment<'a>> {
    type Iter = Cloned<Iter<'a, BorrowedSegment<'a>>>;

    fn segment_iter(&self) -> Self::Iter {
        self.as_slice().iter().cloned()
    }
}

impl<'a, 'b: 'a> Path<'a> for &'b [BorrowedSegment<'a>] {
    type Iter = Cloned<Iter<'a, BorrowedSegment<'a>>>;

    fn segment_iter(&self) -> Self::Iter {
        self.iter().cloned()
    }
}

impl<'a, 'b: 'a, const A: usize> Path<'a> for &'b [BorrowedSegment<'a>; A] {
    type Iter = Cloned<Iter<'a, BorrowedSegment<'a>>>;

    fn segment_iter(&self) -> Self::Iter {
        self.iter().cloned()
    }
}

impl<'a> Path<'a> for &'a str {
    type Iter = JitLookup<'a>;

    fn segment_iter(&self) -> Self::Iter {
        JitPath::new(self).segment_iter()
    }
}

impl<'a> Path<'a> for &'a String {
    type Iter = JitLookup<'a>;

    fn segment_iter(&self) -> Self::Iter {
        JitPath::new(self).segment_iter()
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum OwnedSegment {
    Field(String),
    Index(usize),
    Invalid,
}

impl<'a, 'b: 'a> From<&'b OwnedSegment> for BorrowedSegment<'a> {
    fn from(segment: &'b OwnedSegment) -> Self {
        match segment {
            OwnedSegment::Field(value) => BorrowedSegment::Field(value.as_str()),
            OwnedSegment::Index(value) => BorrowedSegment::Index(*value),
            OwnedSegment::Invalid => BorrowedSegment::Invalid,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum BorrowedSegment<'a> {
    Field(&'a str),
    Index(usize),
    Invalid,
}

impl<'a> From<BorrowedSegment<'a>> for OwnedSegment {
    fn from(x: BorrowedSegment<'a>) -> Self {
        match x {
            BorrowedSegment::Field(value) => OwnedSegment::Field((*value).to_owned()),
            BorrowedSegment::Index(value) => OwnedSegment::Index(value),
            BorrowedSegment::Invalid => OwnedSegment::Invalid,
        }
    }
}