use async_std::prelude::*;
use async_std::stream;
use async_trait::async_trait;
use chrono::prelude::*;
use clap::Parser;
use std::io::ErrorKind;
use std::time::Duration;
use xactor::error::Ok;
use xactor::*;
use yahoo_finance_api as yahoo;

#[derive(Parser, Debug)]
#[clap(
    version = "1.0",
    author = "Claus Matzinger",
    about = "A Manning LiveProject: async Rust"
)]
struct Opts {
    #[clap(short, long, default_value = "AAPL,MSFT,UBER,GOOG")]
    symbols: String,
    #[clap(short, long)]
    from: String,
}

///
/// A trait to provide a common interface for all signal calculations.
///
#[async_trait]
trait AsyncStockSignal {
    ///
    /// The signal's data type.
    ///
    type SignalType;

    ///
    /// Calculate the signal on the provided series.
    ///
    /// # Returns
    ///
    /// The signal (using the provided type) or `None` on error/invalid data.
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType>;
}

///
/// Calculates the absolute and relative difference between the beginning and ending of an f64 series. The relative difference is relative to the beginning.
///
/// # Returns
///
/// A tuple `(absolute, relative)` difference.
///
struct PriceDifference {}

#[async_trait]
impl AsyncStockSignal for PriceDifference {
    type SignalType = (f64, f64);

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() {
            // unwrap is safe here even if first == last
            let (first, last) = (series.first().unwrap(), series.last().unwrap());
            let abs_diff = last - first;
            let first = if *first == 0.0 { 1.0 } else { *first };
            let rel_diff = abs_diff / first;
            Some((abs_diff, rel_diff))
        } else {
            None
        }
    }
}

///
/// Window function to create a simple moving average
///
///
struct WindowedSMA {
    window_size: usize,
}

#[async_trait]
impl AsyncStockSignal for WindowedSMA {
    type SignalType = Vec<f64>;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() && self.window_size > 1 {
            Some(
                series
                    .windows(self.window_size)
                    .map(|w| w.iter().sum::<f64>() / w.len() as f64)
                    .collect(),
            )
        } else {
            None
        }
    }
}

///
/// Find the maximum in a series of f64
///
///
struct MaxPrice {}

#[async_trait]
impl AsyncStockSignal for MaxPrice {
    type SignalType = f64;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MIN, |acc, q| acc.max(*q)))
        }
    }
}

///
/// Find the minimum in a series of f64
///
///
struct MinPrice {}

#[async_trait]
impl AsyncStockSignal for MinPrice {
    type SignalType = f64;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MAX, |acc, q| acc.min(*q)))
        }
    }
}

struct ProcessActor;

#[async_trait]
impl Actor for ProcessActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<ProcessMessage>().await
    }
}

#[async_trait]
impl Handler<ProcessMessage> for ProcessActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: ProcessMessage) {
        println!("process message");
        let line = handle_symbol_data(&msg.symbol, &msg.beginning, &msg.end, msg.data)
            .await
            .unwrap();

        let mut broker = Broker::from_registry().await.unwrap();

        let _result = broker.publish(ToFileMessage { data: line });
    }
}

#[derive(Clone)]
#[message]
struct FetchMessage {
    symbol: String,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

#[derive(Clone)]
#[message]
struct ProcessMessage {
    symbol: String,
    beginning: DateTime<Utc>,
    end: DateTime<Utc>,
    data: Vec<f64>,
}

struct DownloadActor;

#[async_trait]
impl Actor for DownloadActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<FetchMessage>().await
    }
}

#[async_trait]
impl Handler<FetchMessage> for DownloadActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: FetchMessage) -> () {
        println!("fetch message");
        let symbol = msg.symbol.clone();
        let from = msg.from.clone();
        let to = msg.to.clone();

        let quotes = fetch_closing_data(&symbol, &from, &to).await.unwrap();

        let mut broker = Broker::from_registry().await.unwrap();

        let _result = broker.publish(ProcessMessage {
            symbol: quotes.symbol,
            beginning: from,
            end: to,
            data: quotes.quotes,
        });
    }
}

#[derive(Clone)]
#[message]
struct ToFileMessage {
    data: String,
}

struct FileActor {
    file: async_std::fs::File,
}

impl FileActor {
    async fn new() -> FileActor {
        let file = async_std::fs::File::create("data.csv").await.unwrap();
        FileActor { file }
    }
}

#[async_trait]
impl Actor for FileActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        self.file
            .write_all(b"period start,symbol,price,change %,min,max,30d avg\n")
            .await?;

        ctx.subscribe::<ToFileMessage>().await
    }
}

#[async_trait]
impl Handler<ToFileMessage> for FileActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: ToFileMessage) -> () {
        println!("tofile messsage");
        self.file.write_all(msg.data.as_bytes()).await.unwrap();
    }
}

struct Quotes {
    symbol: String,
    quotes: Vec<f64>,
}

///
/// Retrieve data from a data source and extract the closing prices. Errors during download are mapped onto io::Errors as InvalidData.
///
async fn fetch_closing_data(
    symbol: &str,
    beginning: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> std::result::Result<Quotes, std::io::Error> {
    let provider = yahoo::YahooConnector::new();

    let response = provider
        .get_quote_history(symbol, *beginning, *end)
        .await
        .map_err(|_| std::io::Error::from(ErrorKind::InvalidData))?;
    let mut quotes = response
        .quotes()
        .map_err(|_| std::io::Error::from(ErrorKind::InvalidData))?;
    if !quotes.is_empty() {
        quotes.sort_by_cached_key(|k| k.timestamp);
        std::result::Result::Ok(Quotes {
            symbol: symbol.to_string(),
            quotes: quotes.iter().map(|q| q.adjclose as f64).collect(),
        })
    } else {
        std::result::Result::Ok(Quotes {
            symbol: symbol.to_string(),
            quotes: vec![],
        })
    }
}

///
/// Convenience function that chains together the entire processing chain.
///
async fn handle_symbol_data(
    symbol: &str,
    beginning: &DateTime<Utc>,
    _end: &DateTime<Utc>,
    closes: Vec<f64>,
) -> Option<String> {
    if !closes.is_empty() {
        let diff = PriceDifference {};
        let min = MinPrice {};
        let max = MaxPrice {};
        let sma = WindowedSMA { window_size: 30 };

        let period_max: f64 = max.calculate(&closes).await?;
        let period_min: f64 = min.calculate(&closes).await?;

        let last_price = *closes.last()?;
        let (_, pct_change) = diff.calculate(&closes).await?;
        let sma = sma.calculate(&closes).await?;

        // a simple way to output CSV data
        return Some(format!(
            "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}\n",
            beginning.to_rfc3339(),
            symbol,
            last_price,
            pct_change * 100.0,
            period_min,
            period_max,
            sma.last().unwrap_or(&0.0)
        ));
    }
    None
}

#[async_std::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    let from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");

    // a simple way to output a CSV header
    // println!("period start,symbol,price,change %,min,max,30d avg");
    let symbols: Vec<String> = opts.symbols.split(',').map(|s| s.to_string()).collect();

    let _process_addr = ProcessActor.start().await?;

    let _download_addr = DownloadActor.start().await?;

    let _file_addr = FileActor::new().await.start().await?;

    let mut interval = stream::interval(Duration::from_secs(30));
    while let Some(_) = interval.next().await {
        let now = Utc::now();

        for symbol in symbols.clone() {
            Broker::from_registry()
                .await?
                .publish(FetchMessage {
                    symbol,
                    from,
                    to: now,
                })
                .unwrap();
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]
    use super::*;

    #[async_std::test]
    async fn test_PriceDifference_calculate() {
        let signal = PriceDifference {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some((0.0, 0.0)));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some((-1.0, -1.0)));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some((8.0, 4.0))
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some((1.0, 1.0))
        );
    }

    #[async_std::test]
    async fn test_MinPrice_calculate() {
        let signal = MinPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(0.0));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some(1.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(0.0)
        );
    }

    #[async_std::test]
    async fn test_MaxPrice_calculate() {
        let signal = MaxPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(1.0));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some(10.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(6.0)
        );
    }

    #[async_std::test]
    async fn test_WindowedSMA_calculate() {
        let series = vec![2.0, 4.5, 5.3, 6.5, 4.7];

        let signal = WindowedSMA { window_size: 3 };
        assert_eq!(
            signal.calculate(&series).await,
            Some(vec![3.9333333333333336, 5.433333333333334, 5.5])
        );

        let signal = WindowedSMA { window_size: 5 };
        assert_eq!(signal.calculate(&series).await, Some(vec![4.6]));

        let signal = WindowedSMA { window_size: 10 };
        assert_eq!(signal.calculate(&series).await, Some(vec![]));
    }
}
