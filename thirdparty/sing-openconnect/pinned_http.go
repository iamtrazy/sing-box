package openconnect

import (
	"context"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
)

type pinnedHTTPPeer struct {
	address netip.Addr
}

func (p *pinnedHTTPPeer) record(connection net.Conn) {
	address := parseAnyConnectRemoteAddress(connection.RemoteAddr())
	if !address.IsValid() {
		return
	}
	if !p.address.IsValid() {
		p.address = address
	}
}

func newPinnedHTTPClient(
	client *Client,
	serverURL *url.URL,
	acceptedAddress netip.Addr,
	jar http.CookieJar,
	expectedPort uint16,
	protocolName string,
) (*http.Client, *http.Transport, *pinnedHTTPPeer, error) {
	transport := client.httpTransport.Clone()
	expectedHostname := serverURL.Hostname()
	peer := &pinnedHTTPPeer{address: acceptedAddress}
	transport.DialContext = func(ctx context.Context, network string, address string) (net.Conn, error) {
		destinationHostname, destinationPortText, err := net.SplitHostPort(address)
		if err != nil {
			return nil, E.Cause(err, "parse ", protocolName, " accepted endpoint destination")
		}
		destinationPort, err := strconv.ParseUint(destinationPortText, 10, 16)
		if err != nil || destinationPort == 0 {
			return nil, E.New(protocolName, " accepted endpoint destination has an invalid port")
		}
		if !strings.EqualFold(destinationHostname, expectedHostname) || uint16(destinationPort) != expectedPort {
			return nil, E.New(protocolName, " request attempted to dial outside the accepted endpoint")
		}
		// Fork-only fix: upstream substitutes the dial target with the peer address
		// recorded via httptrace.GotConn from the authenticating connection, to pin
		// later requests to the same physical backend. That recorded address is
		// meaningless (it's the proxy's address, not the real server's) whenever the
		// dial is routed through a detour/proxy outbound, which broke every request
		// made through this pinned client when a "detour" is configured. Always
		// dialing by hostname lets client.options.Dialer (which is already
		// detour-aware) resolve consistently instead.
		destination := M.ParseSocksaddrHostPort(expectedHostname, expectedPort)
		connection, dialErr := client.options.Dialer.DialContext(ctx, network, destination)
		if dialErr != nil {
			return nil, E.Cause(dialErr, "dial ", protocolName, " accepted endpoint")
		}
		peer.record(connection)
		return connection, nil
	}
	return &http.Client{
		Transport: client.wrapHTTPTransport(transport),
		Jar:       jar,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}, transport, peer, nil
}
