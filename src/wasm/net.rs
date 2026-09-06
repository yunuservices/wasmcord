use std::net::{IpAddr, Ipv4Addr};

pub(crate) fn is_discord_api_url(url: &str) -> bool {
    url.starts_with("https://discord.com/api/")
        || url.starts_with("https://canary.discord.com/api/")
}

pub(crate) fn is_public_ipv4(addr: &Ipv4Addr) -> bool {
    let [a, b, _, _] = addr.octets();
    !(addr.is_private()
        || addr.is_loopback()
        || addr.is_link_local()
        || addr.is_broadcast()
        || addr.is_documentation()
        || addr.is_unspecified()
        || addr.is_multicast()
        || a == 0
        || (a == 100 && (b & 0xc0) == 64)
        || (a == 192 && b == 0)
        || (a & 0xf0) == 240)
}

pub(crate) fn is_public_ip(addr: &IpAddr) -> bool {
    match addr {
        IpAddr::V4(v4) => is_public_ipv4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => is_public_ipv4(&v4),
            None => {
                !(v6.is_loopback()
                    || v6.is_unspecified()
                    || v6.is_multicast()
                    || (v6.segments()[0] & 0xfe00) == 0xfc00
                    || (v6.segments()[0] & 0xffc0) == 0xfe80)
            }
        },
    }
}

pub(crate) async fn check_outbound_url(url: &str, allowed_hosts: &[String]) -> Result<(), String> {
    let parsed = reqwest::Url::parse(url).map_err(|e| format!("invalid url: {e}"))?;

    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(format!("scheme {scheme} is not permitted"));
    }

    let host = parsed.host_str().ok_or("url has no host")?;
    if !allowed_hosts.iter().any(|allowed| allowed == host) {
        return Err(format!("host {host} is not in http_allowed_hosts"));
    }

    let port = parsed.port_or_known_default().unwrap_or(443);
    let resolved = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| format!("failed to resolve {host}: {e}"))?;

    let mut seen = false;
    for addr in resolved {
        seen = true;
        if !is_public_ip(&addr.ip()) {
            return Err(format!("{host} resolves to a non-public address"));
        }
    }

    if !seen {
        return Err(format!("{host} did not resolve to any address"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_and_metadata_addresses_are_not_public() {
        for addr in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "0.0.0.0",
            "100.64.0.1",
            "::1",
            "fd00::1",
            "fe80::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(
                !is_public_ip(&addr.parse().unwrap()),
                "{addr} should not be treated as public"
            );
        }
    }

    #[test]
    fn routable_addresses_are_public() {
        for addr in ["1.1.1.1", "8.8.8.8", "162.159.128.233", "2606:4700::1111"] {
            assert!(
                is_public_ip(&addr.parse().unwrap()),
                "{addr} should be treated as public"
            );
        }
    }

    #[tokio::test]
    async fn outbound_url_requires_an_allowlisted_host() {
        let allowed = vec!["api.example.com".to_string()];
        assert!(
            check_outbound_url("https://evil.test/x", &allowed)
                .await
                .is_err()
        );
        assert!(
            check_outbound_url("file:///etc/passwd", &allowed)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn outbound_url_rejects_allowlisted_host_on_private_address() {
        let allowed = vec!["localhost".to_string()];
        assert!(
            check_outbound_url("http://localhost:8080/", &allowed)
                .await
                .is_err()
        );
    }
}
