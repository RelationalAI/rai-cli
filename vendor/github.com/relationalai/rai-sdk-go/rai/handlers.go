// Copyright 2022 RelationalAI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rai

// Implementation of the nop and client credentials token handlers.

import (
	"encoding/json"
	"os"
	"os/user"
	"path"
)

type AccessTokenHandler interface {
	GetAccessToken() (string, error)
}

// This handler always returns an empty token, which results in requests not
// being authenticated.
type NopAccessTokenHandler struct{}

func NewNopAccessTokenHandler() NopAccessTokenHandler {
	return NopAccessTokenHandler{}
}

func (h NopAccessTokenHandler) GetAccessToken() (string, error) {
	return "", nil
}

type ClientCredentialsHandler struct {
	client      *Client
	creds       *ClientCredentials
	accessToken *AccessToken
}

// This handler uses the given OAuth client credentials to retrieve access
// tokens, as needed, and caches them locally in ~/.rai/tokens.json.
func NewClientCredentialsHandler(
	c *Client, creds *ClientCredentials,
) ClientCredentialsHandler {
	return ClientCredentialsHandler{client: c, creds: creds}
}

// Returns the name of the token cache file.
func cacheName() (string, error) {
	usr, err := user.Current()
	if err != nil {
		return "", err
	}
	return path.Join(usr.HomeDir, ".rai", "tokens.json"), nil
}

// Read the access token corresponding to the given ClientID from the local
// token cache, returns nil if the token does not exist.
func readAccessToken(creds *ClientCredentials) (*AccessToken, error) {
	cache, err := readTokenCache()
	if err != nil {
		return nil, err
	}
	if token, ok := cache[creds.ClientID]; ok {
		return token, nil
	}
	return nil, nil // doesn't exit
}

func readTokenCache() (map[string]*AccessToken, error) {
	fname, err := cacheName()
	if err != nil {
		return nil, err
	}
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var cache map[string]*AccessToken
	err = json.NewDecoder(f).Decode(&cache)
	if err != nil {
		return nil, err
	}
	return cache, nil
}

// Write the given token to the local token cache.
func writeAccessToken(clientID string, token *AccessToken) {
	cache, err := readTokenCache()
	if err != nil {
		cache = map[string]*AccessToken{}
	}
	cache[clientID] = token
	writeTokenCache(cache)
}

func writeTokenCache(cache map[string]*AccessToken) {
	fname, err := cacheName()
	if err != nil {
		return
	}
	f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	json.NewEncoder(f).Encode(cache)
	f.Close()
}

func (h ClientCredentialsHandler) GetAccessToken() (string, error) {
	// 1. is it already loaded into the handler?
	if h.accessToken != nil && !h.accessToken.IsExpired() {
		return h.accessToken.Token, nil
	}

	// 2. is it available in the tokens.json cache on disk?
	accessToken, err := readAccessToken(h.creds)
	if err == nil && accessToken != nil && !accessToken.IsExpired() {
		h.accessToken = accessToken
		return accessToken.Token, nil
	}

	// 3. request a new token and save in tokens.json cache
	accessToken, err = h.client.GetAccessToken(h.creds)
	if err != nil {
		return "", err
	}
	h.accessToken = accessToken
	writeAccessToken(h.creds.ClientID, accessToken)
	return accessToken.Token, nil
}
