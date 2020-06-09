//
// lib.rs
// Copyright (C) 2020 fx-kirin <fx.kirin@gmail.com>
// Distributed under terms of the MIT license.
//
#![allow(unused_must_use, dead_code)]

use kaniexpect::expect;
use log::{info, trace};
use mio::unix::EventedFd;
use mio::{net::TcpStream, Events, Poll, PollOpt, Ready, Registration, SetReadiness, Token};
use mio_extras::channel;
use std::collections::VecDeque;
use std::io::ErrorKind;
use std::os::unix::io::AsRawFd;
use std::thread;

use std::io::{Read, Write};

pub type Task = (TaskType, Option<usize>, Option<Vec<u8>>);

/// `MyError::source` will return a reference to the `io_error` field

struct SendPending {
    sent_size: usize,
    data: Vec<u8>,
}

struct ReceivePending {
    received_size: usize,
    data: Vec<u8>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TaskType {
    Send,
    Receive,
    Close,
}

enum ReadResult {
    Received(Vec<u8>),
    WouldBlock(ReceivePending),
}

pub struct TcpStreamThread {
    pub stream_thread: Option<std::thread::JoinHandle<()>>,
    pub task_tx: channel::Sender<Task>,
    pub reader_rx: channel::Receiver<Vec<u8>>,
    pub readable_registration: Registration,
    pub reader_events: Events,
    reader_poll: Poll,
}

impl TcpStreamThread {
    pub fn new(tcp_stream: TcpStream) -> Result<Self, std::io::Error> {
        let (task_tx, task_rx) = channel::channel::<Task>();
        let (reader_tx, reader_rx) = channel::channel::<Vec<u8>>();
        let (readable_registration, readable_set_readiness) = Registration::new2();

        let stream_thread = Some(Self::start_tcp_stream_thread(
            tcp_stream,
            task_rx,
            reader_tx,
            readable_set_readiness,
        ));
        let reader_poll = Poll::new()?;
        let reader_events = Events::with_capacity(128);
        reader_poll
            .register(&reader_rx, Token(1001), Ready::readable(), PollOpt::edge())
            .unwrap();
        Ok(Self {
            stream_thread,
            task_tx,
            reader_rx,
            readable_registration,
            reader_poll,
            reader_events,
        })
    }

    pub fn recv(&mut self, size: usize) -> Result<Vec<u8>, anyhow::Error> {
        self.task_tx.send((TaskType::Receive, Some(size), None))?;
        'outer: loop {
            self.reader_poll.poll(&mut self.reader_events, None)?;
            for event in &self.reader_events {
                if event.token() == Token(1001) {
                    break 'outer;
                }
            }
        }
        let result = self.reader_rx.try_recv()?;
        Ok(result)
    }

    pub fn send(&mut self, data: Vec<u8>) -> Result<(), mio_extras::channel::SendError<Task>> {
        self.task_tx.send((TaskType::Send, None, Some(data)))?;
        Ok(())
    }

    pub fn close(&mut self) {
        if self.stream_thread.is_none() {
            info!("Streaming thead not started.");
            return;
        }
        self.task_tx.send((TaskType::Close, None, None));
        self.stream_thread.take().unwrap().join();
        info!("Streaming thead is now closed.");
    }

    fn start_tcp_stream_thread(
        tcp_stream: TcpStream,
        task_rx: channel::Receiver<Task>,
        reader_tx: channel::Sender<Vec<u8>>,
        readable_set_readiness: SetReadiness,
    ) -> std::thread::JoinHandle<()> {
        thread::spawn(move || {
            expect!(Self::thread_loop(
                tcp_stream,
                task_rx,
                reader_tx,
                readable_set_readiness
            ))
        })
    }

    fn thread_loop(
        mut tcp_stream: TcpStream,
        task_rx: channel::Receiver<Task>,
        mut reader_tx: channel::Sender<Vec<u8>>,
        readable_set_readiness: SetReadiness,
    ) -> Result<(), anyhow::Error> {
        let poll = Poll::new()?;
        let fd = &tcp_stream.as_raw_fd();
        let efd = &EventedFd(fd);

        poll.register(
            efd,
            Token(0),
            Ready::readable() | Ready::writable(),
            PollOpt::edge(),
        )?;
        poll.register(&task_rx, Token(1001), Ready::readable(), PollOpt::edge())
            .unwrap();

        let mut events = Events::with_capacity(128);
        let mut is_writable = false;
        let mut is_readable = false;
        let mut sender_queue = VecDeque::<Vec<u8>>::with_capacity(256);
        let mut receiver_queue = VecDeque::<usize>::with_capacity(256);
        let mut send_pending: Option<SendPending> = None;
        let mut receive_pending: Option<ReceivePending> = None;
        'outer: loop {
            poll.poll(&mut events, None)?;
            for event in &events {
                if event.token() == Token(0) {
                    let readiness = event.readiness();
                    if readiness.is_readable() {
                        trace!("Now tcp is readable.");
                        if receive_pending.is_none() && receiver_queue.len() == 0 {
                            readable_set_readiness.set_readiness(Ready::readable());
                        } else {
                            receive_pending = Self::stream_read(
                                &mut tcp_stream,
                                receive_pending.take(),
                                &mut receiver_queue,
                                &mut reader_tx,
                            )?;
                        }
                        is_readable = true;
                    }
                    if readiness.is_writable() {
                        trace!("Now tcp is writable.");
                        is_writable = true;
                        send_pending = Self::stream_write(
                            &mut tcp_stream,
                            send_pending.take(),
                            &mut sender_queue,
                        )?;
                    }
                } else if event.token() == Token(1001) {
                    trace!("Now sender channel is readable.");
                    while let Ok(mut task) = task_rx.try_recv() {
                        match task.0 {
                            TaskType::Send => {
                                sender_queue.push_back(task.2.take().unwrap());
                                if is_writable {
                                    trace!("Sending Data From channel");
                                    send_pending = Self::stream_write(
                                        &mut tcp_stream,
                                        send_pending.take(),
                                        &mut sender_queue,
                                    )?;
                                }
                            }
                            TaskType::Receive => {
                                receiver_queue.push_back(task.1.unwrap());
                                if is_readable {
                                    receive_pending = Self::stream_read(
                                        &mut tcp_stream,
                                        receive_pending.take(),
                                        &mut receiver_queue,
                                        &mut reader_tx,
                                    )?;
                                }
                            }
                            TaskType::Close => {
                                info!("Thread will be closing.");
                                break 'outer Ok(());
                            }
                        }
                    }
                }
            }
        }
    }

    fn stream_read(
        tcp_stream: &mut TcpStream,
        mut receive_pending: Option<ReceivePending>,
        receiver_queue: &mut VecDeque<usize>,
        mut reader_tx: &mut channel::Sender<Vec<u8>>,
    ) -> Result<Option<ReceivePending>, std::io::Error> {
        if receive_pending.is_some() {
            let receive_pending = receive_pending.take().unwrap();
            let result = Self::read_all(tcp_stream, receive_pending);
            match result {
                Ok(ReadResult::Received(received)) => {
                    reader_tx.send(received);
                }
                Ok(ReadResult::WouldBlock(receive_pending)) => return Ok(Some(receive_pending)),
                Err(e) => {
                    return Err(e);
                }
            }
        }
        while let Some(size) = receiver_queue.pop_front() {
            trace!("receiveing from queue");
            let receive_pending = ReceivePending {
                received_size: 0,
                data: vec![0; size],
            };
            let result = Self::read_all(tcp_stream, receive_pending);
            match result {
                Ok(ReadResult::Received(received)) => {
                    reader_tx.send(received);
                }
                Ok(ReadResult::WouldBlock(receive_pending)) => return Ok(Some(receive_pending)),
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(None)
    }

    fn read_all(
        tcp_stream: &mut TcpStream,
        mut receive_pending: ReceivePending,
    ) -> Result<ReadResult, std::io::Error> {
        loop {
            let result =
                tcp_stream.read(&mut receive_pending.data[receive_pending.received_size..]);
            match result {
                Ok(v) => {
                    receive_pending.received_size += v;
                    if receive_pending.received_size == receive_pending.data.len() {
                        return Ok(ReadResult::Received(receive_pending.data));
                    } else if receive_pending.received_size > receive_pending.data.len() {
                        let e = std::io::Error::new(
                            ErrorKind::Other,
                            format!(
                                "Received size greater than expected! expected:{} actual:{} v:{}",
                                receive_pending.received_size,
                                receive_pending.data.len(),
                                v
                            ),
                        );
                        return Err(e.into());
                    }
                }
                Err(e) => match e.kind() {
                    ErrorKind::WouldBlock => {
                        return Ok(ReadResult::WouldBlock(receive_pending));
                    }
                    _ => return Err(e.into()),
                },
            }
        }
    }

    fn stream_write(
        tcp_stream: &mut TcpStream,
        mut send_pending: Option<SendPending>,
        sender_queue: &mut VecDeque<Vec<u8>>,
    ) -> Result<Option<SendPending>, std::io::Error> {
        if send_pending.is_some() {
            let send_pending = send_pending.take().unwrap();
            let result = Self::write_all(tcp_stream, send_pending);
            match result {
                Ok(None) => {}
                Ok(send_pending) => return Ok(send_pending),
                Err(e) => {
                    return Err(e);
                }
            }
        }
        while let Some(data) = sender_queue.pop_front() {
            trace!("Sending from queue");
            let send_pending = SendPending {
                sent_size: 0,
                data: data,
            };
            let result = Self::write_all(tcp_stream, send_pending);
            match result {
                Ok(None) => {}
                Ok(send_pending) => return Ok(send_pending),
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(None)
    }

    fn write_all(
        tcp_stream: &mut TcpStream,
        mut send_pending: SendPending,
    ) -> Result<Option<SendPending>, std::io::Error> {
        loop {
            let result = tcp_stream.write(&send_pending.data[send_pending.sent_size..]);
            match result {
                Ok(v) => {
                    send_pending.sent_size += v;
                    if send_pending.sent_size == send_pending.data.len() {
                        break;
                    } else {
                        let e = std::io::Error::new(
                            ErrorKind::Other,
                            "Received size greater than expected!",
                        );
                        return Err(e.into());
                    }
                }
                Err(e) => match e.kind() {
                    ErrorKind::WouldBlock => {
                        return Ok(Some(send_pending));
                    }
                    _ => return Err(e.into()),
                },
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() -> Result<(), anyhow::Error> {
        let conn = TcpStream::connect(&"127.0.0.1:9999".parse()?)?;
        let mut stream = TcpStreamThread::new(conn)?;
        stream.send(b"test".to_vec())?;
        let result = stream.recv(4)?;
        assert_eq!(result, b"TEST".to_vec());
        stream.close();
        Ok(())
    }
}
