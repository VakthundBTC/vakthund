//! The routing table (`docs/modules/net/README.md`, "Routing table"). A command missing from
//! [`Command`] has no owner and its messages are dropped.

use bitcoin_p2p_messages::message::{CommandString, CommandStringError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Owner {
    Net,
    BlockSync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Version,
    Verack,
    SendHeaders,
    Ping,
    Pong,
    Headers,
    Block,
    Inv,
    NotFound,
    GetHeaders,
    GetData,
}

impl Command {
    const ALL: [Self; 11] = [
        Self::Version,
        Self::Verack,
        Self::SendHeaders,
        Self::Ping,
        Self::Pong,
        Self::Headers,
        Self::Block,
        Self::Inv,
        Self::NotFound,
        Self::GetHeaders,
        Self::GetData,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Version => "version",
            Self::Verack => "verack",
            Self::SendHeaders => "sendheaders",
            Self::Ping => "ping",
            Self::Pong => "pong",
            Self::Headers => "headers",
            Self::Block => "block",
            Self::Inv => "inv",
            Self::NotFound => "notfound",
            Self::GetHeaders => "getheaders",
            Self::GetData => "getdata",
        }
    }

    /// `None`: the command has no owner.
    pub fn parse(command: &CommandString) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|known| known.name() == command.as_ref())
    }

    pub fn to_command_string(self) -> Result<CommandString, CommandStringError> {
        CommandString::try_from(self.name())
    }

    pub fn owner(self) -> Owner {
        match self {
            Self::Version | Self::Verack | Self::SendHeaders | Self::Ping | Self::Pong => {
                Owner::Net
            }
            Self::Headers
            | Self::Block
            | Self::Inv
            | Self::NotFound
            | Self::GetHeaders
            | Self::GetData => Owner::BlockSync,
        }
    }
}
