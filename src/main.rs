extern crate ctrlc;
extern crate json;
extern crate ws;

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::{Arc, mpsc, Mutex};
use std::thread;

const LOG_PATH_PREFIX: &'static str = "";

struct Tracker {
    trade_file: Arc<Mutex<File>>,
    //book_file: Arc<Mutex<File>>,
}

impl Tracker {
    fn new(name: &str, url: &str, interrupt: mpsc::Sender<()>) -> Tracker {
        let url = String::from(url);

        let trade_file = Arc::new(Mutex::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&format!("{}{}_trade", LOG_PATH_PREFIX, name))
                .unwrap()
        ));

        /*let book_file = Arc::new(Mutex::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&format!("{}_book", name))
                .unwrap()
        ));*/

        {
            let trade_file = trade_file.clone();
            //let book_file = book_file.clone();

            thread::spawn(move || {
                let _ = ws::connect(url, |_| {
                    |message| {
                        if let ws::Message::Text(text) = message {
                            let object = json::parse(&text).unwrap();

                            if object["type"] == "update" && object["timestampms"].is_number() {
                                for event in object["events"].members() {
                                    if event["type"] == "trade" {
                                        let _ = write!(
                                            trade_file.lock().unwrap(),
                                            "{} {} {} {}\n",
                                            object["timestampms"],
                                            event["price"],
                                            event["amount"],
                                            if event["makerSide"] == "ask" {
                                                0
                                            } else {
                                                1
                                            },
                                        );
                                    } //else if event["type"] == "change" {
                                    //}
                                }
                            }
                        }

                        Ok(())
                    }
                });

                let _ = interrupt.send(());
            });
        }

        Tracker {
            trade_file: trade_file,
            //book_file: book_file,
        }
    }

    fn close(self) {
        let _ = self.trade_file.lock().unwrap().flush();
        //let _ = self.book_file.lock().unwrap().flush();
    }
}

fn main() {
    let (interrupt_sender, interrupt_receiver) = mpsc::channel();

    let trackers = vec![
        Tracker::new("gemini_btcusd", "wss://api.gemini.com/v1/marketdata/BTCUSD?heartbeat=true", interrupt_sender.clone()),
        Tracker::new("gemini_ethusd", "wss://api.gemini.com/v1/marketdata/ETHUSD?heartbeat=true", interrupt_sender.clone()),
    ];

    ctrlc::set_handler(move || {
        let _ = interrupt_sender.send(());
    }).unwrap();

    let _ = interrupt_receiver.recv();

    for tracker in trackers {
        tracker.close();
    }
}
