#[allow(deprecated)]
use solana_program::{
    decode_error::DecodeError, msg, program_error::{PrintProgramError, ProgramError},
};
use thiserror::Error;
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PumpError {
    #[error("The given account is not authorized to execute this instruction.")]
    NotAuthorized = 6000,
    #[error("The program is already initialized.")]
    AlreadyInitialized = 6001,
    #[error("slippage: Too much SOL required to buy the given amount of tokens.")]
    TooMuchSolRequired = 6002,
    #[error("slippage: Too little SOL received to sell the given amount of tokens.")]
    TooLittleSolReceived = 6003,
    #[error("The mint does not match the bonding curve.")]
    MintDoesNotMatchBondingCurve = 6004,
    #[error("The bonding curve has completed and liquidity migrated to raydium.")]
    BondingCurveComplete = 6005,
    #[error("The bonding curve has not completed.")]
    BondingCurveNotComplete = 6006,
    #[error("The program is not initialized.")]
    NotInitialized = 6007,
}
impl From<PumpError> for ProgramError {
    fn from(e: PumpError) -> Self {
        ProgramError::Custom(e as u32)
    }
}
#[allow(deprecated)]
impl<T> DecodeError<T> for PumpError {
    fn type_of() -> &'static str {
        "PumpError"
    }
}
#[allow(deprecated)]
impl PrintProgramError for PumpError {
    fn print<E>(&self)
    where
        E: 'static + std::error::Error + DecodeError<E> + PrintProgramError
            + num_traits::FromPrimitive,
    {
        msg!(& self.to_string());
    }
}

impl num_traits::FromPrimitive for PumpError {
    fn from_i64(n: i64) -> Option<Self> {
        match n {
            6000 => Some(PumpError::NotAuthorized),
            6001 => Some(PumpError::AlreadyInitialized),
            6002 => Some(PumpError::TooMuchSolRequired),
            6003 => Some(PumpError::TooLittleSolReceived),
            6004 => Some(PumpError::MintDoesNotMatchBondingCurve),
            6005 => Some(PumpError::BondingCurveComplete),
            6006 => Some(PumpError::BondingCurveNotComplete),
            6007 => Some(PumpError::NotInitialized),
            _ => None,
        }
    }

    fn from_u64(n: u64) -> Option<Self> {
        match n {
            6000 => Some(PumpError::NotAuthorized),
            6001 => Some(PumpError::AlreadyInitialized),
            6002 => Some(PumpError::TooMuchSolRequired),
            6003 => Some(PumpError::TooLittleSolReceived),
            6004 => Some(PumpError::MintDoesNotMatchBondingCurve),
            6005 => Some(PumpError::BondingCurveComplete),
            6006 => Some(PumpError::BondingCurveNotComplete),
            6007 => Some(PumpError::NotInitialized),
            _ => None,
        }
    }
}
