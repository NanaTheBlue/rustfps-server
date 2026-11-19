use std::{error::Error, fmt};

use crate::net::transport::MAX_DATAGRAM_SIZE;

pub const PROTOCOL_ID: u32 = 0x5246_5053; // "RFPS"
pub const HEADER_SIZE: usize = 9;

const TYPE_CONNECT: u8 = 1;
const TYPE_DISCONNECT: u8 = 2;
const TYPE_CONNECT_ACK: u8 = 3;

#[derive(Debug, PartialEq, Eq)]
pub enum PacketError {
    TooShort,
    TooLarge,
    InvalidProtocol(u32),
    UnknownPacketType(u8),
    InvalidPayloadLength,
}

impl fmt::Display for PacketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooShort => write!(f, "packet shorter than header ({HEADER_SIZE} bytes)"),
            Self::TooLarge => write!(f, "packet exceeds {MAX_DATAGRAM_SIZE} bytes"),
            Self::InvalidProtocol(id) => write!(f, "invalid protocol id: {id:#010x}"),
            Self::UnknownPacketType(ty) => write!(f, "unknown packet type: {ty}"),
            Self::InvalidPayloadLength => write!(f, "invalid payload length"),
        }
    }
}

impl Error for PacketError {}

#[derive(Debug, PartialEq, Eq)]
pub struct Packet {
    pub header: PacketHeader,
    pub payload: PacketPayload,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PacketHeader {
    pub protocol_id: u32,
    pub sequence_number: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PacketPayload {
    Connect,
    Disconnect,
    ConnectAck { client_id: u32 },
}

impl Packet {
    pub fn new(sequence_number: u32, payload: PacketPayload) -> Self {
        Self {
            header: PacketHeader {
                protocol_id: PROTOCOL_ID,
                sequence_number,
            },
            payload,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, PacketError> {
        let (packet_type, payload_len) = match self.payload {
            PacketPayload::Connect => (TYPE_CONNECT, 0),
            PacketPayload::Disconnect => (TYPE_DISCONNECT, 0),
            PacketPayload::ConnectAck { .. } => (TYPE_CONNECT_ACK, 4),
        };

        let total_len = HEADER_SIZE + payload_len;
        if total_len > MAX_DATAGRAM_SIZE {
            return Err(PacketError::TooLarge);
        }

        let mut buf = Vec::with_capacity(total_len);
        buf.extend_from_slice(&self.header.protocol_id.to_be_bytes());
        buf.extend_from_slice(&self.header.sequence_number.to_be_bytes());
        buf.push(packet_type);

        match self.payload {
            PacketPayload::Connect | PacketPayload::Disconnect => {}
            PacketPayload::ConnectAck { client_id } => {
                buf.extend_from_slice(&client_id.to_be_bytes());
            }
        }

        Ok(buf)
    }

    pub fn decode(mut bytes: &[u8]) -> Result<Self, PacketError> {
        if bytes.len() > MAX_DATAGRAM_SIZE {
            return Err(PacketError::TooLarge);
        }
        if bytes.len() < HEADER_SIZE {
            return Err(PacketError::TooShort);
        }

        let protocol_id = take_u32(&mut bytes)?;
        if protocol_id != PROTOCOL_ID {
            return Err(PacketError::InvalidProtocol(protocol_id));
        }

        let sequence_number = take_u32(&mut bytes)?;
        let packet_type = take_u8(&mut bytes)?;

        let payload = match packet_type {
            TYPE_CONNECT => PacketPayload::Connect,
            TYPE_DISCONNECT => PacketPayload::Disconnect,
            TYPE_CONNECT_ACK => {
                let client_id =
                    take_u32(&mut bytes).map_err(|_| PacketError::InvalidPayloadLength)?;
                PacketPayload::ConnectAck { client_id }
            }
            other => return Err(PacketError::UnknownPacketType(other)),
        };

        if !bytes.is_empty() {
            return Err(PacketError::InvalidPayloadLength);
        }

        Ok(Self {
            header: PacketHeader {
                protocol_id,
                sequence_number,
            },
            payload,
        })
    }
}

fn take_u8(input: &mut &[u8]) -> Result<u8, PacketError> {
    let (&byte, rest) = input.split_first().ok_or(PacketError::TooShort)?;
    *input = rest;
    Ok(byte)
}

fn take_u32(input: &mut &[u8]) -> Result<u32, PacketError> {
    let (&bytes, rest) = input
        .split_first_chunk::<4>()
        .ok_or(PacketError::TooShort)?;
    *input = rest;
    Ok(u32::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_packets() {
        let cases = [
            (Packet::new(1, PacketPayload::Connect), HEADER_SIZE),
            (Packet::new(2, PacketPayload::Disconnect), HEADER_SIZE),
            (
                Packet::new(3, PacketPayload::ConnectAck { client_id: 42 }),
                HEADER_SIZE + 4,
            ),
        ];

        for (packet, expected_len) in cases {
            let encoded = packet.encode().unwrap();
            assert_eq!(encoded.len(), expected_len);

            let decoded = Packet::decode(&encoded).unwrap();
            assert_eq!(decoded, packet);
        }
    }

    #[test]
    fn rejects_invalid_packets() {
        assert_eq!(Packet::decode(&[]), Err(PacketError::TooShort));
        assert_eq!(
            Packet::decode(&[0; HEADER_SIZE - 1]),
            Err(PacketError::TooShort)
        );
        assert_eq!(
            Packet::decode(&vec![0; MAX_DATAGRAM_SIZE + 1]),
            Err(PacketError::TooLarge)
        );

        let mut bad_proto = Packet::new(1, PacketPayload::Connect).encode().unwrap();
        bad_proto[0] ^= 0xFF;
        assert!(matches!(
            Packet::decode(&bad_proto),
            Err(PacketError::InvalidProtocol(_))
        ));

        let mut bad_type = Packet::new(1, PacketPayload::Connect).encode().unwrap();
        bad_type[8] = 99;
        assert_eq!(
            Packet::decode(&bad_type),
            Err(PacketError::UnknownPacketType(99))
        );

        let mut truncated_ack = Packet::new(1, PacketPayload::ConnectAck { client_id: 7 })
            .encode()
            .unwrap();
        truncated_ack.truncate(HEADER_SIZE + 2);
        assert_eq!(
            Packet::decode(&truncated_ack),
            Err(PacketError::InvalidPayloadLength)
        );

        let mut trailing_bytes = Packet::new(1, PacketPayload::Connect).encode().unwrap();
        trailing_bytes.push(0x01);
        assert_eq!(
            Packet::decode(&trailing_bytes),
            Err(PacketError::InvalidPayloadLength)
        );
    }
}
