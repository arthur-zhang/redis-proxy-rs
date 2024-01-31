#[derive(Debug, PartialEq)]
pub enum DecodeError {
    InvalidProtocol,
    NotEnoughData,
    UnexpectedErr,
    IOError,
}

impl From<std::io::Error> for DecodeError {
    fn from(e: std::io::Error) -> Self {
        DecodeError::IOError
    }
}

impl From<anyhow::Error> for DecodeError {
    fn from(e: anyhow::Error) -> Self {
        DecodeError::UnexpectedErr
    }
}

