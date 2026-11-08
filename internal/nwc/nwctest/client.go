// Package nwctest is a small NWC client for tests. It lives here so that go-nostr
// stays inside internal/nwc. It writes its own requests from the protocol, and an
// outside client checks our reading of that protocol later.
package nwctest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/getAlby/go-nostr"
	"github.com/getAlby/go-nostr/nip04"
	"github.com/getAlby/go-nostr/nip44"
)

// Encryption schemes.
const (
	NIP44 = "nip44_v2"
	NIP04 = "nip04"
	// NoTag sends NIP-04 with no encryption tag, as old clients do.
	NoTag = ""
)

// Client holds a parsed connection string.
type Client struct {
	Secret     string
	Public     string
	ServicePub string
	RelayURL   string

	relay *nostr.Relay
}

// Parse reads a connection string.
func Parse(connectionString string) (*Client, error) {
	parsed, err := url.Parse(connectionString)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "nostr+walletconnect" {
		return nil, fmt.Errorf("wrong scheme %q", parsed.Scheme)
	}
	c := &Client{ServicePub: parsed.Host, RelayURL: parsed.Query().Get("relay")}
	if len(c.ServicePub) != 64 || c.RelayURL == "" {
		return nil, fmt.Errorf("incomplete connection string %q", connectionString)
	}
	return c, c.setSecret(parsed.Query().Get("secret"))
}

func (c *Client) setSecret(secret string) error {
	public, err := nostr.GetPublicKey(secret)
	if err != nil {
		return fmt.Errorf("secret: %w", err)
	}
	c.Secret, c.Public = secret, public
	return nil
}

// Stranger returns a client for the same service with a random key.
func (c *Client) Stranger() (*Client, error) {
	stranger := &Client{ServicePub: c.ServicePub, RelayURL: c.RelayURL}
	return stranger, stranger.setSecret(nostr.GeneratePrivateKey())
}

// Response is a decrypted reply.
type Response struct {
	ResultType string          `json:"result_type"`
	Error      *ResponseError  `json:"error"`
	Result     json.RawMessage `json:"result"`
}

// ResponseError is the error in a reply.
type ResponseError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Seal signs a request with the given content, already encrypted.
func (c *Client) Seal(content string, createdAt nostr.Timestamp, tags ...nostr.Tag) (*nostr.Event, error) {
	event := &nostr.Event{
		Kind: 23194, CreatedAt: createdAt, Content: content,
		Tags: append(nostr.Tags{{"p", c.ServicePub}}, tags...),
	}
	return event, event.Sign(c.Secret)
}

// Request builds a signed request with no params.
func (c *Client) Request(scheme, method string, createdAt nostr.Timestamp, tags ...nostr.Tag) (*nostr.Event, error) {
	return c.RequestWith(scheme, method, struct{}{}, createdAt, tags...)
}

// RequestWith builds a signed request.
func (c *Client) RequestWith(
	scheme, method string, params any, createdAt nostr.Timestamp, tags ...nostr.Tag,
) (*nostr.Event, error) {
	body, err := json.Marshal(map[string]any{"method": method, "params": params})
	if err != nil {
		return nil, err
	}
	content, err := c.Encrypt(scheme, string(body))
	if err != nil {
		return nil, err
	}
	if scheme != NoTag {
		tags = append(tags, nostr.Tag{"encryption", scheme})
	}
	return c.Seal(content, createdAt, tags...)
}

// Encrypt encrypts to the service key. Any scheme but NIP-44 is NIP-04.
func (c *Client) Encrypt(scheme, plain string) (string, error) {
	if scheme == NIP44 {
		key, err := nip44.GenerateConversationKey(c.ServicePub, c.Secret)
		if err != nil {
			return "", err
		}
		return nip44.Encrypt(plain, key)
	}
	key, err := nip04.ComputeSharedSecret(c.ServicePub, c.Secret)
	if err != nil {
		return "", err
	}
	return nip04.Encrypt(plain, key)
}

// Open checks and decrypts a reply to the given request.
func (c *Client) Open(scheme string, request, reply *nostr.Event) (*Response, error) {
	if reply.Kind != 23195 || reply.PubKey != c.ServicePub || !reply.CheckID() {
		return nil, fmt.Errorf("not a reply from the service: %v", reply)
	}
	if valid, err := reply.CheckSignature(); err != nil || !valid {
		return nil, fmt.Errorf("bad signature on reply: %w", err)
	}
	if !hasTag(reply, "p", c.Public) || !hasTag(reply, "e", request.ID) {
		return nil, fmt.Errorf("reply is not tagged for this request: %v", reply.Tags)
	}
	plain, err := c.Decrypt(scheme, reply.Content)
	if err != nil {
		return nil, fmt.Errorf("decrypt reply: %w", err)
	}
	var response Response
	if err := json.Unmarshal([]byte(plain), &response); err != nil {
		return nil, fmt.Errorf("read reply %q: %w", plain, err)
	}
	return &response, nil
}

// Decrypt decrypts from the service key. Any scheme but NIP-44 is NIP-04.
func (c *Client) Decrypt(scheme, content string) (string, error) {
	if scheme == NIP44 {
		key, err := nip44.GenerateConversationKey(c.ServicePub, c.Secret)
		if err != nil {
			return "", err
		}
		return nip44.Decrypt(content, key)
	}
	key, err := nip04.ComputeSharedSecret(c.ServicePub, c.Secret)
	if err != nil {
		return "", err
	}
	return nip04.Decrypt(content, key)
}

func hasTag(event *nostr.Event, name, value string) bool {
	for _, tag := range event.Tags {
		if len(tag) >= 2 && tag[0] == name && tag[1] == value {
			return true
		}
	}
	return false
}

// Connect opens the relay.
func (c *Client) Connect(ctx context.Context) error {
	if c.relay != nil {
		_ = c.relay.Close()
	}
	relay, err := nostr.RelayConnect(ctx, c.RelayURL)
	if err != nil {
		return err
	}
	c.relay = relay
	return nil
}

// Close closes the relay.
func (c *Client) Close() {
	if c.relay != nil {
		_ = c.relay.Close()
	}
}

// Info is a service's info event.
type Info struct {
	Methods    []string
	Encryption string
	Tags       []string
}

// ReadInfo reads the service's info event from the relay.
func (c *Client) ReadInfo(ctx context.Context) (*Info, error) {
	events, err := c.relay.QuerySync(ctx, nostr.Filter{Kinds: []int{13194}, Authors: []string{c.ServicePub}, Limit: 1})
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, errors.New("no info event on the relay")
	}
	info := &Info{Methods: strings.Fields(events[0].Content)}
	for _, tag := range events[0].Tags {
		if len(tag) >= 2 && tag[0] == "encryption" {
			info.Encryption = tag[1]
		}
		if len(tag) >= 1 {
			info.Tags = append(info.Tags, tag[0])
		}
	}
	return info, nil
}

// Call sends a request with no params and waits for the reply until ctx ends.
func (c *Client) Call(ctx context.Context, scheme, method string) (*Response, error) {
	return c.CallWith(ctx, scheme, method, struct{}{})
}

// CallWith sends a request and waits for the reply until ctx ends.
func (c *Client) CallWith(ctx context.Context, scheme, method string, params any) (*Response, error) {
	request, err := c.RequestWith(scheme, method, params, nostr.Now())
	if err != nil {
		return nil, err
	}
	subscription, err := c.relay.Subscribe(ctx, nostr.Filters{{
		Kinds: []int{23195}, Authors: []string{c.ServicePub}, Tags: nostr.TagMap{"e": {request.ID}},
	}})
	if err != nil {
		return nil, err
	}
	defer subscription.Unsub()
	if err := c.relay.Publish(ctx, *request); err != nil {
		return nil, fmt.Errorf("publish: %w", err)
	}
	select {
	case reply, open := <-subscription.Events:
		if !open {
			return nil, errors.New("subscription ended")
		}
		return c.Open(scheme, request, reply)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// NewKey returns a random secret key and its public key, both in hex.
func NewKey() (secret, public string) {
	secret = nostr.GeneratePrivateKey()
	public, _ = nostr.GetPublicKey(secret)
	return secret, public
}

// SignEvent returns an event signed with the secret key, in JSON.
func SignEvent(secret string, kind int, createdAt int64, tags [][]string, content string) ([]byte, error) {
	event := nostr.Event{Kind: kind, CreatedAt: nostr.Timestamp(createdAt), Content: content}
	for _, tag := range tags {
		event.Tags = append(event.Tags, nostr.Tag(tag))
	}
	if err := event.Sign(secret); err != nil {
		return nil, err
	}
	return json.Marshal(event)
}
