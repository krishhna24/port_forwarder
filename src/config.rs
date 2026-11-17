//! Validated runtime configuration.

use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use crate::error::{Error, Result};

/// Default maximum number of simultaneously forwarded connections.
pub const DEFAULT_MAX_CONNECTIONS: usize = 1024;
/// Default timeout for establishing an upstream connection.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Default inactivity timeout after which a connection is torn down.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
/// Default time to wait for in-flight connections during graceful shutdown.
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
/// Default size of each per-direction copy buffer.
pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;
/// Smallest accepted copy buffer.
pub const MIN_BUFFER_SIZE: usize = 1024;
/// Largest accepted copy buffer.
pub const MAX_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// Fully validated forwarder configuration.
///
/// Construct it with [`Config::new`] and adjust fields as needed; call
/// [`Config::validate`] (done automatically by the forwarder) before use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Address to listen on, as `host:port`. Port `0` picks an ephemeral port.
    pub listen: String,
    /// Upstream address, as `host:port`. Re-resolved for every connection.
    pub upstream: String,
    /// Upper bound on simultaneously forwarded connections.
    pub max_connections: usize,
    /// Timeout for each upstream connection attempt.
    pub connect_timeout: Duration,
    /// Connection-wide inactivity timeout; `None` disables it.
    pub idle_timeout: Option<Duration>,
    /// How long graceful shutdown waits for in-flight connections.
    pub shutdown_timeout: Duration,
    /// Size of each per-direction copy buffer in bytes.
    pub buffer_size: usize,
}

impl Config {
    /// Creates a configuration with default tuning for the given addresses.
    pub fn new(listen: impl Into<String>, upstream: impl Into<String>) -> Self {
        Self {
            listen: listen.into(),
            upstream: upstream.into(),
            max_connections: DEFAULT_MAX_CONNECTIONS,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            idle_timeout: Some(DEFAULT_IDLE_TIMEOUT),
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }

    /// Checks every field for consistency without touching the network.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] describing the first invalid field.
    pub fn validate(&self) -> Result<()> {
        validate_host_port("listen", &self.listen, true)?;
        validate_host_port("upstream", &self.upstream, false)?;
        if self.max_connections == 0 {
            return Err(invalid("max-connections must be at least 1"));
        }
        if self.connect_timeout.is_zero() {
            return Err(invalid("connect-timeout must be greater than zero"));
        }
        if self.idle_timeout.is_some_and(|t| t.is_zero()) {
            return Err(invalid(
                "idle-timeout must be greater than zero (use None / `0` on the CLI to disable)",
            ));
        }
        if !(MIN_BUFFER_SIZE..=MAX_BUFFER_SIZE).contains(&self.buffer_size) {
            return Err(invalid(format!(
                "buffer-size must be between {MIN_BUFFER_SIZE} and {MAX_BUFFER_SIZE} bytes, got {}",
                self.buffer_size
            )));
        }
        Ok(())
    }
}

/// Resolves `addr` to one or more socket addresses.
///
/// # Errors
///
/// Returns [`Error::Resolve`] if resolution fails or yields no addresses.
pub fn resolve(role: &'static str, addr: &str) -> Result<Vec<SocketAddr>> {
    let resolve_err = |source| Error::Resolve {
        role,
        addr: addr.to_owned(),
        source,
    };
    let addrs: Vec<SocketAddr> = addr.to_socket_addrs().map_err(resolve_err)?.collect();
    if addrs.is_empty() {
        return Err(resolve_err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no addresses returned",
        )));
    }
    Ok(addrs)
}

fn invalid(msg: impl Into<String>) -> Error {
    Error::InvalidConfig(msg.into())
}

/// Syntactic check of `host:port` (hostnames, IPv4 and bracketed IPv6).
fn validate_host_port(role: &str, value: &str, allow_port_zero: bool) -> Result<()> {
    let Some((host, port)) = value.rsplit_once(':') else {
        return Err(invalid(format!(
            "{role} address `{value}` must be in host:port form"
        )));
    };
    if host.is_empty() {
        return Err(invalid(format!(
            "{role} address `{value}` is missing a host"
        )));
    }
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        return Err(invalid(format!(
            "{role} address `{value}`: IPv6 hosts must be bracketed, e.g. [::1]:8080"
        )));
    }
    let port: u16 = port
        .parse()
        .map_err(|_| invalid(format!("{role} address `{value}` has an invalid port")))?;
    if port == 0 && !allow_port_zero {
        return Err(invalid(format!("{role} port must not be 0")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> Config {
        Config::new("127.0.0.1:8080", "127.0.0.1:9000")
    }

    #[test]
    fn defaults_are_valid() {
        valid().validate().unwrap();
    }

    #[test]
    fn accepts_hostnames_ipv6_and_ephemeral_listen_port() {
        let mut c = valid();
        c.listen = "[::1]:0".into();
        c.upstream = "example.com:443".into();
        c.validate().unwrap();
    }

    #[test]
    fn rejects_malformed_addresses() {
        for bad in [
            "127.0.0.1",
            ":80",
            "host:",
            "host:http",
            "host:70000",
            "::1:80",
        ] {
            let mut c = valid();
            c.upstream = bad.into();
            assert!(
                matches!(c.validate(), Err(Error::InvalidConfig(_))),
                "{bad} should be rejected"
            );
        }
    }

    #[test]
    fn rejects_upstream_port_zero() {
        let mut c = valid();
        c.upstream = "127.0.0.1:0".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_out_of_range_tuning() {
        let mut c = valid();
        c.max_connections = 0;
        assert!(c.validate().is_err());

        let mut c = valid();
        c.connect_timeout = Duration::ZERO;
        assert!(c.validate().is_err());

        let mut c = valid();
        c.idle_timeout = Some(Duration::ZERO);
        assert!(c.validate().is_err());

        let mut c = valid();
        c.buffer_size = MIN_BUFFER_SIZE - 1;
        assert!(c.validate().is_err());

        let mut c = valid();
        c.buffer_size = MAX_BUFFER_SIZE + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn idle_timeout_can_be_disabled() {
        let mut c = valid();
        c.idle_timeout = None;
        c.validate().unwrap();
    }

    #[test]
    fn resolves_literal_and_localhost() {
        assert_eq!(
            resolve("upstream", "127.0.0.1:9000").unwrap(),
            vec!["127.0.0.1:9000".parse().unwrap()]
        );
        assert!(!resolve("upstream", "localhost:9000").unwrap().is_empty());
    }

    #[test]
    fn resolve_failure_names_the_role() {
        let err = resolve("upstream", "no-such-host.invalid:80").unwrap_err();
        assert!(err.to_string().contains("upstream"), "{err}");
    }
}
