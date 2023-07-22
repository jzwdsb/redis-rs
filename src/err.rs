use std::fmt::Display;

#[derive(Debug, PartialEq, Clone)]
#[allow(dead_code)]
pub enum Err {
    WrongType,
    SyntaxError,
    IOError(String),
    NoKey,
    NoValue,
    NoIndex,
    NoMember,
    NoScore,
    NoStart,
    NoStop,
    NoOffset,
    NoLimit,
    NoCommand,
    NoField,
    NoIncrement,
    NoDecrement,
    NoFloat,
    NoInteger,
    NoDouble,
    NoString,
    NoList,
    NoSet,
    NoHash,
    NoZSet,
    NoRange,
    NoOrder,
    NoAggregate,
    NoWeights,
    NoKeys,
    NoDestination,
    NoSource,
    NoTimeout,
    NoPattern,
    NoCount,
    NoCursor,
    NoMatch,
    NoMin,
    NoMax,
    NoLexMin,
    NoLexMax,
    NoIndexMin,
    NoIndexMax,
    NoIndexOffset,
    NoIndexLimit,
    NoIndexValue,
    NoIndexScore,
    NoIndexMember,
    NoIndexStart,
    NoIndexStop,
    NoIndexField,
    NoIndexIncrement,
    NoIndexDecrement,
    NoIndexFloat,
    NoIndexInteger,
    NoIndexDouble,
    NoIndexString,
    NoIndexList,
    NoIndexSet,
    NoIndexHash,
    NoIndexZSet,
    NoIndexRange,
    NoIndexOrder,
    NoIndexAggregate,
    NoIndexWeights,
    NoIndexKeys,
    NoIndexDestination,
    NoIndexSource,
    NoIndexTimeout,
    NoIndexPattern,
    NoIndexCount,
    NoIndexCursor,
    NoIndexMatch,
    NoIndexLexMin,
    NoIndexLexMax,
    NoIndexCommand,
    NoIndexFieldIncrement,
    NoIndexFieldDecrement,
    NoIndexFieldFloat,
    NoIndexFieldInteger,
    NoIndexFieldDouble,
    NoIndexFieldString,
    NoIndexFieldList,
    NoIndexFieldSet,
    NoIndexFieldHash,
    NoIndexFieldZSet,
    NoIndexFieldRange,
    NoIndexFieldOrder,
    NoIndexFieldAggregate,
    NoIndexFieldWeights,
    NoIndexFieldKeys,
    NoIndexFieldDestination,
    NoIndexFieldSource,
    NoIndexFieldTimeout,
    NoIndexFieldPattern,
    NoIndexFieldCount,
    NoIndexFieldCursor,
    NoIndexFieldMatch,
    NoIndexFieldMin,
    NoIndexFieldMax,
}

impl std::error::Error for Err {}

impl Display for Err {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{:?}", self).as_str())
    }
}

impl From<std::io::Error> for Err {
    fn from(err: std::io::Error) -> Self {
        Err::IOError(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Err {
    fn from(_: std::string::FromUtf8Error) -> Self {
        Err::SyntaxError
    }
}

impl From<std::num::ParseIntError> for Err {
    fn from(_: std::num::ParseIntError) -> Self {
        Err::SyntaxError
    }
}

impl Into<String> for Err {
    fn into(self) -> String {
        std::fmt::format(format_args!("{:?}", self))
    }
}
