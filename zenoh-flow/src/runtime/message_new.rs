//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

extern crate serde;

use crate::{ZFData, ZFError, ZFResult};
use async_std::sync::Arc;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use uhlc::Timestamp;
use uuid::Uuid;

use std::time::Duration;

// ser/de
use byteorder::{LittleEndian, WriteBytesExt}; //,ReadBytesExt};
                                              //use std::io::Cursor;
use zenoh_buffers::{WBuf, ZBuf, ZBufReader};

use zenoh_buffers::reader::HasReader;
use zenoh_buffers::traits::buffer::{CopyBuffer, InsertBuffer};
use zenoh_buffers::traits::reader::Reader;

pub type NodeId = u64;
pub type PortId = u64;
pub type RuntimeId = u64;
pub type FlowId = u64;
pub type PortType = u64;

pub const DATA: u8 = 0x01;
pub const RECORD_START: u8 = 0x02;
pub const RECORD_STOP: u8 = 0x03;

pub const E_FLAG: u8 = 1 << 7;
pub const M_FLAG: u8 = 1 << 6;
pub const L_FLAG: u8 = 1 << 5;

/// # Data Message
/// ```text
///  7 6 5 4 3 2 1 0
/// +-+-+-+-+-+-+-+-+
/// |E|M|L|  DATA   | # DATA=0x00
/// +---------------+
/// ~   timestamp   ~
/// +---------------+
/// ~ E2E deadlines ~ if E==1
/// +---------------+
/// ~ E2D dead miss ~ if M==1
/// +---------------+
/// ~   loop ctxs   ~ if L==1
/// +---------------+
/// ~     [u8]      ~
/// +---------------+
/// ```
///

/// # Recording Start Message
/// ```text
///  7 6 5 4 3 2 1 0
/// +-+-+-+-+-+-+-+-+
/// |X|X|X| RECSTART| # RECSTART=0x01
/// +---------------+
/// ~   timestamp   ~
/// +---------------+
/// ~    PortId     ~ # U64
/// +---------------+
/// ~    NodeId     ~ # U64
/// +---------------+
/// ~    FlowId     ~ # U64
/// +---------------+
/// ~  InstanceId   ~ # [u8,16]
/// +---------------+
/// ```

/// # Recording Stop Message
/// ```text
///  7 6 5 4 3 2 1 0
/// +-+-+-+-+-+-+-+-+
/// |X|X|X| RECSTOP | # RECSTOP=0x02
/// +---------------+
/// ~   timestamp   ~
/// +---------------+
/// ```

#[derive(Clone)]
pub enum ZenohFlowMessage {
    Data(DataMessage),
    RecordStart(RecordingStart),
    RecordStop(RecordingStop),
}

impl ZenohFlowMessage {
    #[inline(always)]
    pub fn serialize_wbuf(&self, buff: &mut WBuf) -> bool {
        match &self {
            Self::Data(d) => d.serialize_custom_wbuf(buff),
            _ => panic!("Not yet")
            // Self::RecordStart(d) => d.serialize_custom_wbuf(buff),
            // Self::RecordStop(d) => d.serialize_custom_wbuf(buff),
        }
    }

    #[inline(always)]
    pub fn serialize_to_vec<IsWriter>(&self, buff: &mut IsWriter) -> bool
    where
        IsWriter: WriteBytesExt,
    {
        match &self {
            Self::Data(d) => d.serialize_custom_writer(buff),
            _ => panic!("Not yet")
            // Self::RecordStart(d) => d.serialize_custom_wbuf(buff),
            // Self::RecordStop(d) => d.serialize_custom_wbuf(buff),
        }
    }
}

#[derive(Clone)]
pub struct RecordingStop(Timestamp);
impl RecordingStop {
    pub fn new(ts: Timestamp) -> Self {
        Self(ts)
    }
}

// impl ZFMessage for RecordingStop {
//     #[inline(always)]
//     fn serialize_custom_wbuf(&self, buff: &mut WBuf) -> bool {
//         buff.write(RECORD_STOP) && buff.write_timestamp(&self.0)
//     }
// }

#[derive(Clone)]
pub struct RecordingStart {
    pub ts: Timestamp,
    pub port_id: PortId,
    pub node_id: NodeId,
    pub flow_id: FlowId,
    pub instance_id: Uuid,
}

impl RecordingStart {
    pub fn new(
        ts: Timestamp,
        port_id: PortId,
        node_id: NodeId,
        flow_id: FlowId,
        instance_id: Uuid,
    ) -> Self {
        Self {
            ts,
            port_id,
            node_id,
            flow_id,
            instance_id,
        }
    }
}

// impl ZFMessage for RecordingStart {
//     #[inline(always)]
//     fn serialize_custom_wbuf(&self, buff: &mut WBuf) -> bool {
//         buff.write(RECORD_START)
//             && buff.write_timestamp(&self.ts)
//             && buff.write_bytes(self.port_id.to_le_bytes().as_slice())
//             && buff.write_bytes(self.node_id.to_le_bytes().as_slice())
//             && buff.write_bytes(self.flow_id.to_le_bytes().as_slice())
//             && buff.write_bytes_array(self.instance_id.as_bytes())
//     }
// }

// Timestamp is uhlc::HLC::Timestamp (u64, usize, [u8;16])
// All the Ids are Arc<str>
// Uuid is uuid::Uuid ([u8;16])
// Duration is std::Duration (u64,u32)

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataMessage {
    pub(crate) data: Data,
    pub(crate) timestamp: Timestamp,
    pub(crate) end_to_end_deadlines: Vec<E2EDeadline>,
    pub(crate) missed_end_to_end_deadlines: Vec<E2EDeadlineMiss>,
    pub(crate) loop_contexts: Vec<LoopContext>,
}

impl DataMessage {
    pub fn new(data: Data, timestamp: Timestamp, end_to_end_deadlines: Vec<E2EDeadline>) -> Self {
        Self {
            data,
            timestamp,
            end_to_end_deadlines,
            missed_end_to_end_deadlines: vec![],
            loop_contexts: vec![],
        }
    }

    /// Returns a mutable reference over the Data representation (i.e. `Bytes` or `Typed`).
    ///
    /// This method should be called in conjonction with `try_get::<Typed>()` in order to get a
    /// reference of the desired type. For instance:
    ///
    /// `let zf_usise: &ZFUsize = data_message.get_inner_data().try_get::<ZFUsize>()?;`
    ///
    /// Note that the prerequisite for the above code to work is that `ZFUsize` implements the
    /// traits: `ZFData` and `Deserializable`.
    pub fn get_inner_data(&mut self) -> &mut Data {
        &mut self.data
    }

    pub fn get_timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    pub fn get_missed_end_to_end_deadlines(&self) -> &[E2EDeadlineMiss] {
        self.missed_end_to_end_deadlines.as_slice()
    }

    pub fn get_loop_contexts(&self) -> &[LoopContext] {
        self.loop_contexts.as_slice()
    }

    pub fn new_serialized(
        data: Arc<Vec<u8>>,
        timestamp: Timestamp,
        end_to_end_deadlines: Vec<E2EDeadline>,
        loop_contexts: Vec<LoopContext>,
    ) -> Self {
        Self {
            data: Data::Bytes(data),
            timestamp,
            end_to_end_deadlines,
            missed_end_to_end_deadlines: vec![],
            loop_contexts,
        }
    }

    pub fn new_deserialized(
        data: Arc<dyn ZFData>,
        timestamp: Timestamp,
        end_to_end_deadlines: Vec<E2EDeadline>,
        loop_contexts: Vec<LoopContext>,
    ) -> Self {
        Self {
            data: Data::Typed(data),
            timestamp,
            end_to_end_deadlines,
            missed_end_to_end_deadlines: vec![],
            loop_contexts,
        }
    }
}

impl ZFMessage for DataMessage {
    #[inline(always)]
    fn serialize_custom_wbuf(&self, buff: &mut WBuf) -> bool {
        let mut header = DATA;
        if !self.end_to_end_deadlines.is_empty() {
            header |= E_FLAG;
        }
        if !self.missed_end_to_end_deadlines.is_empty() {
            header |= M_FLAG;
        }
        if !self.loop_contexts.is_empty() {
            header |= L_FLAG;
        }

        let data_bytes = self.data.try_as_bytes().unwrap();

        // header
        buff.write_byte(header);

        // timestamp
        buff.write(&self.timestamp.get_time().as_u64().to_le_bytes());
        buff.write(self.timestamp.get_id().as_slice());

        // data
        buff.write(&data_bytes.len().to_le_bytes());
        buff.append(data_bytes);

        true
    }

    #[inline(always)]
    fn serialize_custom_writer<Writer>(&self, buff: &mut Writer) -> bool
    where
        Writer: WriteBytesExt,
    {
        let mut header = DATA;
        if !self.end_to_end_deadlines.is_empty() {
            header |= E_FLAG;
        }
        if !self.missed_end_to_end_deadlines.is_empty() {
            header |= M_FLAG;
        }
        if !self.loop_contexts.is_empty() {
            header |= L_FLAG;
        }

        let data_bytes = self.data.try_as_bytes().unwrap();
        // writer.write_i32::<LittleEndian>(self.identifier).unwrap();
        buff.write_u8(header).is_ok()
            && buff
                .write_u64::<LittleEndian>(self.timestamp.get_time().as_u64())
                .is_ok()
            && buff.write_all(self.timestamp.get_id().as_slice()).is_ok()
            && buff
                .write_u64::<LittleEndian>(data_bytes.len() as u64)
                .is_ok()
            && buff.write_all(&data_bytes).is_ok()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Data {
    Bytes(Arc<Vec<u8>>),
    #[serde(skip_serializing, skip_deserializing)]
    // Typed data is never serialized
    Typed(Arc<dyn ZFData>),
}

impl Data {
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self::Bytes(Arc::new(bytes))
    }

    pub fn try_as_bytes(&self) -> ZFResult<Arc<Vec<u8>>> {
        match &self {
            Self::Bytes(bytes) => Ok(bytes.clone()),
            Self::Typed(typed) => {
                let serialized_data = typed
                    .try_serialize()
                    .map_err(|_| ZFError::SerializationError)?;
                Ok(Arc::new(serialized_data))
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct E2EDeadline {
    pub duration: Duration,
    pub from: OutputDescriptor,
    pub to: InputDescriptor,
    pub start: Timestamp,
}

// impl ZFMessage for E2EDeadline {
//     fn serialize_custom_wbuf(&self, buff: &mut WBuf) -> bool {
//         buff.write_bytes(self.duration.as_secs().to_le_bytes().as_slice())
//             && buff.write_bytes(self.duration.subsec_nanos().to_le_bytes().as_slice())
//             && self.from.serialize_custom_wbuf(buff)
//             && self.to.serialize_custom_wbuf(buff)
//             && buff.write_timestamp(&self.start)
//     }
// }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct E2EDeadlineMiss {
    pub duration: Duration,
    pub from: OutputDescriptor,
    pub to: InputDescriptor,
    pub start: Timestamp,
    pub end: Timestamp,
}

// impl ZFMessage for E2EDeadlineMiss {
//     fn serialize_custom_wbuf(&self, buff: &mut WBuf) -> bool {
//         buff.write_bytes(self.duration.as_secs().to_le_bytes().as_slice())
//             && buff.write_bytes(self.duration.subsec_nanos().to_le_bytes().as_slice())
//             && self.from.serialize_custom_wbuf(buff)
//             && self.to.serialize_custom_wbuf(buff)
//             && buff.write_timestamp(&self.start)
//             && buff.write_timestamp(&self.end)
//     }
// }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoopContext {
    pub(crate) ingress: NodeId,
    pub(crate) egress: NodeId,
    pub(crate) iteration: LoopIteration,
    pub(crate) timestamp_start_first_iteration: Timestamp,
    pub(crate) timestamp_start_current_iteration: Option<Timestamp>,
    pub(crate) duration_last_iteration: Option<Duration>,
}

// impl ZFMessage for LoopContext {
//     fn serialize_custom_wbuf(&self, buff: &mut WBuf) -> bool {
//         buff.write_bytes(self.ingress.to_le_bytes().as_slice())
//             && buff.write_bytes(self.egress.to_le_bytes().as_slice())
//             && buff.write_timestamp(&self.timestamp_start_first_iteration)
//     }
// }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LoopIteration {
    Finite(u64),
    Infinite,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InputDescriptor {
    pub node: NodeId,
    pub input: PortId,
}

// impl ZFMessage for InputDescriptor {
//     fn serialize_custom_wbuf(&self, buff: &mut WBuf) -> bool {
//         buff.write_bytes(self.node.to_le_bytes().as_slice())
//             && buff.write_bytes(self.input.to_le_bytes().as_slice())
//     }
// }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputDescriptor {
    pub node: NodeId,
    pub output: PortId,
}

// impl ZFMessage for OutputDescriptor {
//     fn serialize_custom_wbuf(&self, buff: &mut WBuf) -> bool {
//         buff.write_bytes(self.node.to_le_bytes().as_slice())
//             && buff.write_bytes(self.output.to_le_bytes().as_slice())
//     }

// }

pub trait ZFMessage {
    fn serialize_custom_wbuf(&self, buff: &mut WBuf) -> bool;
    fn serialize_custom_writer<Writer>(&self, buff: &mut Writer) -> bool
    where
        Writer: WriteBytesExt;
}

pub fn deserialize_custom_zbuf(data: &mut ZBuf, buff: &mut [u8]) -> ZFResult<ZenohFlowMessage> {
    let mut reader = data.reader();

    let header = reader.read_byte().ok_or(ZFError::GenericError)?;

    if header & DATA == DATA {
        let _e = header & E_FLAG;
        let _m = header & M_FLAG;
        let _l = header & L_FLAG;

        // time stamp
        let ts = read_timestamp(&mut reader)?;

        let payload = read_payload(&mut reader, buff)?;

        return Ok(ZenohFlowMessage::Data(DataMessage::new_serialized(
            payload,
            ts,
            vec![],
            vec![],
        )));
    }
    if header & RECORD_START == RECORD_START {
        return Err(ZFError::DeseralizationError);
    }
    if header & RECORD_STOP == RECORD_STOP {
        return Err(ZFError::DeseralizationError);
    }
    Err(ZFError::DeseralizationError)
}

pub fn read_timestamp(reader: &mut ZBufReader) -> ZFResult<Timestamp> {
    let mut id_buf = [0_u8; 16];

    let ntp = read_u64(reader)?;

    reader
        .read_exact(&mut id_buf)
        .then(|| 0)
        .ok_or(ZFError::GenericError)?;

    Ok(Timestamp::new(uhlc::NTP64(ntp), uhlc::ID::new(16, id_buf)))
}

pub fn read_u64(reader: &mut ZBufReader) -> ZFResult<u64> {
    let mut u64_buf = [0_u8; 8];
    reader
        .read_exact(&mut u64_buf)
        .then(|| 0)
        .ok_or(ZFError::GenericError)?;
    let value = u64::from_le_bytes(u64_buf);

    Ok(value)
}

pub fn read_payload(reader: &mut ZBufReader, buff: &mut [u8]) -> ZFResult<Arc<Vec<u8>>> {
    let size = read_u64(reader)? as usize;

    if reader.read(buff) == size as usize {
        let payload = Arc::new(buff[..size].iter().cloned().collect());

        return Ok(payload);
    }

    Err(ZFError::DeseralizationError)
}

#[cfg(test)]
mod tests {
    use crate::runtime::message_new::{
        deserialize_custom_zbuf, read_payload, read_timestamp, Data as NewData,
        DataMessage as NewDataMessage, ZenohFlowMessage, DATA,
    };
    use crate::ZFError;
    use async_std::sync::Arc;
    use zenoh::net::protocol::io::*;
    use zenoh_buffers::reader::HasReader;
    use zenoh_buffers::traits::reader::Reader;

    #[test]
    fn test_ser_de() {
        let hlc = Arc::new(uhlc::HLC::default());
        let payload_data: Vec<u8> = vec![];
        let ts = hlc.new_timestamp();
        let data_new = NewData::from_bytes(payload_data.clone());
        let data_msg_new = NewDataMessage::new(data_new, ts, vec![]);
        let zf_msg_new = ZenohFlowMessage::Data(data_msg_new);
        let mut wbuf = WBuf::new(256, false);
        let mut buff = Vec::with_capacity(65_535);

        zf_msg_new.serialize_wbuf(&mut wbuf);

        let mut zbuf: ZBuf = wbuf.into();

        let mut reader = zbuf.reader();

        let header = reader.read_byte().ok_or(ZFError::GenericError).unwrap();

        assert_eq!(header, DATA);

        assert_eq!(header & DATA, DATA);

        let r_ts = read_timestamp(&mut reader).unwrap();

        assert_eq!(r_ts, ts);

        let r_payload = read_payload(&mut reader, &mut buff).unwrap();

        assert_eq!(payload_data, *r_payload);

        reader.reset();

        let mut buff2 = Vec::with_capacity(65_535);

        let de = deserialize_custom_zbuf(&mut zbuf, &mut buff2).is_ok();

        assert!(de);
    }
}
