//! pub/sub comamnd

use std::pin::Pin;

use super::*;
use crate::connection::AsyncConnection;
use crate::db::DB;
use crate::frame::Frame;
use crate::Result;

use bytes::Bytes;
use log::{error, warn};
use tokio::{select, sync::broadcast};
use tokio_stream::{Stream, StreamExt, StreamMap};

#[derive(Debug)]
pub struct Publish {
    channel: String,
    message: Bytes,
}

impl Publish {
    fn new(channel: String, message: Bytes) -> Self {
        Self { channel, message }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"PUBLISH")?;
        let channel = next_string(&mut iter)?; // channel
        let message = next_bytes(&mut iter)?; // message
        Ok(Self::new(channel, message))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        Frame::Integer(db.publish(self.channel, self.message) as i64)
    }
} // impl Publish

// stream of messages
// the stream receive messages from the boardcast receiver
// stream! macro is used to create a stream
// Pin is used to pin the stream to the memory, the address of the stream will not change
// Box is used to box the stream
// send is used to send the stream to other threads
// sync is used to share the stream between threads
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send + Sync>>;

#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
    cmd_parser: Parser,
}

impl Subscribe {
    fn new(channels: Vec<String>) -> Self {
        Self {
            channels,
            cmd_parser: Parser::new(),
        }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"SUBSCRIBE")?;
        let mut channels = Vec::new();
        for next in iter {
            match next {
                Frame::SimpleString(channel) => {
                    channels.push(channel);
                }
                Frame::BulkString(channel) => {
                    channels.push(String::from_utf8(channel.to_vec())?);
                }
                _ => return Err(RedisErr::FrameMalformed),
            }
        }
        Ok(Self::new(channels))
    }

    // after subscribe, the connection will be blocked
    // and the crruent running task/thread will be blocked
    // until the connection is unsubscribed
    pub async fn apply(
        mut self,
        db: &mut DB,
        dst: &mut AsyncConnection,
        shutdown: Arc<Notify>,
    ) -> Frame {
        let mut subscriptions: StreamMap<String, Messages> = StreamMap::new();

        // Subscribe to the channel.
        // infinate loop until
        // 1. the connection is unsubscribed
        // 2. the connection is closed
        // 3. the server is shutdown
        loop {
            for channel_name in self.channels.drain(..) {
                if let Err(e) =
                    subscribe_channel(channel_name, &mut subscriptions, db.clone(), dst).await
                {
                    return Frame::Error(e.to_string());
                }
            }

            select! {
                Some((channel_name, msg)) = subscriptions.next() => {
                    trace!("received message from channel: {}", channel_name);
                    dst.write_frame(make_message_frame(channel_name, msg)).await.unwrap();
                }
                res = dst.read_frame() => {
                    match res {
                        Ok(frame) => {
                            if let Err(e) = self.handle_command(frame, &mut subscriptions, dst).await {
                                error!("Error handling command: {}", e);
                                return Frame::Error(e.to_string());
                            }
                        },
                        Err(_) => return Frame::Nil,
                    };
                }
                _ = shutdown.notified() => {
                    return Frame::Nil;
                }
            }
        }
    }

    async fn handle_command(
        &mut self,
        frames: Frame,
        subscriptions: &mut StreamMap<String, Messages>,
        dst: &mut AsyncConnection,
    ) -> Result<()> {
        match self.cmd_parser.parse(frames)? {
            Command::Unsubscribe(mut cmd) => {
                if cmd.channels().is_empty() {
                    cmd.channels = subscriptions.keys().cloned().collect();
                }
                for channel in cmd.channels() {
                    subscriptions.remove(channel);
                    let response = make_unsubscribe_frame(channel.clone(), subscriptions.len());
                    dst.write_frame(response).await?;
                }
            }

            Command::Subscribe(cmd) => self.channels.extend(cmd.channels),
            cmd => {
                warn!(
                    "could not handle command in subscribe, dropped, received cmd: {:?}",
                    cmd
                );
            }
        }

        Ok(())
    }
} // impl Subscribe

async fn subscribe_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Messages>,
    db: DB,
    dst: &mut AsyncConnection,
) -> Result<()> {
    let mut rx = db.subscribe(channel_name.clone());

    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                // yield message
                Ok(msg) => yield msg,
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });

    subscriptions.insert(channel_name.clone(), rx);

    let response = make_subscribe_frame(channel_name, subscriptions.len());
    dst.write_frame(response).await?;

    Ok(())
}

fn make_message_frame(channel_name: String, message: Bytes) -> Frame {
    Frame::Array(vec![
        Frame::BulkString(Bytes::from_static(b"message")),
        Frame::BulkString(Bytes::from(channel_name)),
        Frame::BulkString(message),
    ])
}

fn make_subscribe_frame(channel_name: String, num_subscriptions: usize) -> Frame {
    Frame::Array(vec![
        Frame::BulkString(Bytes::from_static(b"message")),
        Frame::BulkString(Bytes::from(channel_name)),
        Frame::Integer(num_subscriptions as i64),
    ])
}

fn make_unsubscribe_frame(channel_name: String, num_subscriptions: usize) -> Frame {
    Frame::Array(vec![
        Frame::BulkString(Bytes::from_static(b"unsubscribe")),
        Frame::BulkString(Bytes::from(channel_name)),
        Frame::Integer(num_subscriptions as i64),
    ])
}

// Unsubscribe from a channel
// can only be used after subscribe
#[derive(Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

impl Unsubscribe {
    fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"UNSUBSCRIBE")?;
        let mut channels = Vec::new();
        for next in iter {
            match next {
                Frame::SimpleString(channel) => {
                    channels.push(channel);
                }
                Frame::BulkString(channel) => {
                    channels.push(String::from_utf8(channel.to_vec())?);
                }
                _ => return Err(RedisErr::FrameMalformed),
            }
        }
        Ok(Self::new(channels))
    }

    pub fn apply(self, _db: &mut DB) -> Frame {
        todo!("Unsubscribe command should be handled by the Subscribe command")
    }

    pub fn channels(&self) -> &[String] {
        &self.channels
    }
} // impl Unsubscribe

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_frames() {
        let cmd = Publish::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"PUBLISH")),
            Frame::BulkString(Bytes::from_static(b"channel")),
            Frame::BulkString(Bytes::from_static(b"message")),
        ]);
        assert_eq!(
            cmd.unwrap().channel,
            Publish::new("channel".to_string(), Bytes::from("message")).channel
        );

        let cmd = Subscribe::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"SUBSCRIBE")),
            Frame::BulkString(Bytes::from_static(b"channel")),
        ]);
        assert_eq!(
            cmd.unwrap().channels,
            Subscribe::new(vec!["channel".to_string()]).channels
        );
    }
}
