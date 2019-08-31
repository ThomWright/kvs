use failure;
use std::convert::TryFrom;
use std::convert::TryInto;

/// Was this worth it? Maybe not. Maybe a type alias would have been fine.
#[derive(Debug, Clone, Copy)]
pub struct Bytes(pub u64);

impl std::ops::Add<Bytes> for Bytes {
    type Output = Bytes;
    fn add(self, rhs: Bytes) -> Self::Output {
        Bytes(self.0 + rhs.0)
    }
}

impl std::ops::Add<&Bytes> for Bytes {
    type Output = Bytes;
    fn add(self, rhs: &Bytes) -> Self::Output {
        Bytes(self.0 + rhs.0)
    }
}

impl std::ops::AddAssign<Bytes> for Bytes {
    fn add_assign(&mut self, rhs: Bytes) {
        self.0 += rhs.0
    }
}

impl std::ops::AddAssign<&Bytes> for Bytes {
    fn add_assign(&mut self, rhs: &Bytes) {
        self.0 += rhs.0
    }
}

impl std::ops::Sub<Bytes> for Bytes {
    type Output = Bytes;
    fn sub(self, rhs: Bytes) -> Self::Output {
        Bytes(self.0 - rhs.0)
    }
}

impl TryFrom<usize> for Bytes {
    type Error = failure::Error;
    fn try_from(n: usize) -> Result<Self, Self::Error> {
        Ok(Bytes(n.try_into()?))
    }
}
