//! Command-line interface definition and value parsers.

use std::time::Duration;

use clap::Parser;

use crate::config::{Config, DEFAULT_MAX_CONNECTIONS};
use crate::error::Result;
use crate::logging::LogFormat;

// clap needs `&'static str` defaults; `cli_defaults_match_config_defaults`
// guarantees these stay in sync with the constants in `config`.
const DEFAULT_CONNECT_TIMEOUT_STR: &str = "5s";
const DEFAULT_IDLE_TIMEOUT_STR: &str = "300s";
const DEFAULT_SHUTDOWN_TIMEOUT_STR: &str = "30s";
const DEFAULT_BUFFER_SIZE_STR: &str = "64KiB";

/// A lightweight, protocol-agnostic TCP port forwarder.
///
/// Every accepted client gets its own upstream connection; raw bytes are
/// relayed in both directions and TCP half-close is preserved.
#[derive(Debug, Parser)]
#[command(name = "tcp-forwarder", version, about, long_about = None)]
pub struct Args {
    /// Address to listen on (host:port). Hostnames are resolved at startup.
    #[arg(short, long, value_name = "HOST:PORT")]
    pub listen: String,

    /// Upstream address to forward to (host:port). Re-resolved per connection.
    #[arg(short, long, value_name = "HOST:PORT")]
    pub upstream: String,

    /// Maximum simultaneous connections; extra clients are closed immediately.
    #[arg(long, value_name = "N", default_value_t = DEFAULT_MAX_CONNECTIONS)]
    pub max_connections: usize,

    /// Timeout for each upstream connection attempt (e.g. 500ms, 5s, 1m).
    #[arg(long, value_name = "DURATION", value_parser = parse_duration,
          default_value = DEFAULT_CONNECT_TIMEOUT_STR)]
    pub connect_timeout: Duration,

    /// Tear down a connection after no bytes move in either direction for this long; 0 disables.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration,
          default_value = DEFAULT_IDLE_TIMEOUT_STR)]
    pub idle_timeout: Duration,

    /// On SIGINT/SIGTERM, wait this long for in-flight connections before forcing them closed.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration,
          default_value = DEFAULT_SHUTDOWN_TIMEOUT_STR)]
    pub shutdown_timeout: Duration,

    /// Per-direction copy buffer size (e.g. 16KiB, 64KiB, 1MiB).
    #[arg(long, value_name = "SIZE", value_parser = parse_size,
          default_value = DEFAULT_BUFFER_SIZE_STR)]
    pub buffer_size: usize,

    /// Log output format; verbosity is controlled with `RUST_LOG` (default: info).
    #[arg(long, value_enum, default_value_t = LogFormat::Text)]
    pub log_format: LogFormat,
}

impl Args {
    /// Converts parsed arguments into a validated [`Config`].
    ///
    /// # Errors
    ///
    /// Returns an error if any value fails [`Config::validate`].
    pub fn into_config(self) -> Result<Config> {
        let config = Config {
            listen: self.listen,
            upstream: self.upstream,
            max_connections: self.max_connections,
            connect_timeout: self.connect_timeout,
            idle_timeout: (!self.idle_timeout.is_zero()).then_some(self.idle_timeout),
            shutdown_timeout: self.shutdown_timeout,
            buffer_size: self.buffer_size,
        };
        config.validate()?;
        Ok(config)
    }
}

/// Parses durations such as `250ms`, `5s`, `2m`, `1h`; a bare number means seconds.
///
/// # Errors
///
/// Returns a human-readable message for malformed input.
pub fn parse_duration(input: &str) -> Result<Duration, String> {
    let s = input.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let value: u64 = num
        .parse()
        .map_err(|_| format!("invalid duration `{input}` (expected e.g. 500ms, 5s, 2m)"))?;
    let per_unit_ms: u64 = match unit.trim() {
        "ms" => 1,
        "" | "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        other => {
            return Err(format!(
                "unknown duration unit `{other}` (use ms, s, m or h)"
            ));
        }
    };
    value
        .checked_mul(per_unit_ms)
        .map(Duration::from_millis)
        .ok_or_else(|| format!("duration `{input}` is too large"))
}

/// Parses sizes such as `4096`, `16K`, `64KiB`, `1M`, `1MiB` (binary units).
///
/// # Errors
///
/// Returns a human-readable message for malformed input.
pub fn parse_size(input: &str) -> Result<usize, String> {
    let s = input.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let value: usize = num
        .parse()
        .map_err(|_| format!("invalid size `{input}` (expected e.g. 16KiB, 64KiB, 1MiB)"))?;
    let multiplier: usize = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kib" => 1024,
        "m" | "mib" => 1024 * 1024,
        other => return Err(format!("unknown size unit `{other}` (use B, KiB or MiB)")),
    };
    value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size `{input}` is too large"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        DEFAULT_BUFFER_SIZE, DEFAULT_CONNECT_TIMEOUT, DEFAULT_IDLE_TIMEOUT,
        DEFAULT_SHUTDOWN_TIMEOUT,
    };
    use clap::CommandFactory;

    #[test]
    fn clap_definition_is_consistent() {
        Args::command().debug_assert();
    }

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(parse_duration("7").unwrap(), Duration::from_secs(7));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("0").unwrap(), Duration::ZERO);
        for bad in ["", "s", "-1s", "1.5s", "5 days", "99999999999999999999h"] {
            assert!(parse_duration(bad).is_err(), "{bad} should fail");
        }
    }

    #[test]
    fn parses_sizes() {
        assert_eq!(parse_size("4096").unwrap(), 4096);
        assert_eq!(parse_size("16K").unwrap(), 16 * 1024);
        assert_eq!(parse_size("64KiB").unwrap(), 64 * 1024);
        assert_eq!(parse_size("1MiB").unwrap(), 1024 * 1024);
        for bad in ["", "KiB", "1GiB", "1.5K"] {
            assert!(parse_size(bad).is_err(), "{bad} should fail");
        }
    }

    #[test]
    fn cli_defaults_match_config_defaults() {
        assert_eq!(
            parse_duration(DEFAULT_CONNECT_TIMEOUT_STR).unwrap(),
            DEFAULT_CONNECT_TIMEOUT
        );
        assert_eq!(
            parse_duration(DEFAULT_IDLE_TIMEOUT_STR).unwrap(),
            DEFAULT_IDLE_TIMEOUT
        );
        assert_eq!(
            parse_duration(DEFAULT_SHUTDOWN_TIMEOUT_STR).unwrap(),
            DEFAULT_SHUTDOWN_TIMEOUT
        );
        assert_eq!(
            parse_size(DEFAULT_BUFFER_SIZE_STR).unwrap(),
            DEFAULT_BUFFER_SIZE
        );
    }

    #[test]
    fn minimal_args_produce_default_config() {
        let args = Args::try_parse_from([
            "tcp-forwarder",
            "--listen",
            "127.0.0.1:8080",
            "--upstream",
            "127.0.0.1:9000",
        ])
        .unwrap();
        assert_eq!(
            args.into_config().unwrap(),
            Config::new("127.0.0.1:8080", "127.0.0.1:9000")
        );
    }

    #[test]
    fn zero_idle_timeout_disables_it() {
        let args = Args::try_parse_from([
            "tcp-forwarder",
            "-l",
            "127.0.0.1:8080",
            "-u",
            "127.0.0.1:9000",
            "--idle-timeout",
            "0",
            "--buffer-size",
            "16KiB",
        ])
        .unwrap();
        let config = args.into_config().unwrap();
        assert_eq!(config.idle_timeout, None);
        assert_eq!(config.buffer_size, 16 * 1024);
    }

    #[test]
    fn missing_required_args_fail() {
        assert!(Args::try_parse_from(["tcp-forwarder", "--listen", "127.0.0.1:1"]).is_err());
    }

    #[test]
    fn invalid_values_fail_validation() {
        let args =
            Args::try_parse_from(["tcp-forwarder", "-l", "127.0.0.1:8080", "-u", "nope"]).unwrap();
        assert!(args.into_config().is_err());
    }
}
