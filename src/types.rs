use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub type Term = u64;

pub type ServerId = u64;

/**
 * Trait Explanations
 * 1. Serialize -> Serde json serialization
 * 2. DeserializeOwned -> Serde json deserialize to a new instance
 * 3. Send -> Can transfer safely across threads
 * 4. 'static -> Lifetime specifier. Will live as long as the program (unsure about this)
 * 5. Debug -> Used by std lib for printing
 * 6. Clone -> Can create a duplicate of itself
 */
pub trait RaftTypeTrait:
    Serialize
    + for<'de> Deserialize<'de>
    + DeserializeOwned
    + Send
    + 'static
    + Debug
    + Clone
    + FromStr
    + Display
{
}
impl<
        T: Serialize
            + for<'de> Deserialize<'de>
            + DeserializeOwned
            + Send
            + 'static
            + Debug
            + Clone
            + FromStr
            + Display,
    > RaftTypeTrait for T
{
}
