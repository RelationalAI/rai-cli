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

import (
	"encoding/json"
	"io"
	"time"
)

// todo: make sure CreatedOn is persisted as epoch seconds
type AccessToken struct {
	Token     string `json:"access_token"`
	Scope     string `json:"scope"`
	ExpiresIn int    `json:"expires_in"` // token lifetime in seconds
	CreatedOn int64  `json:"created_on"` // epoch seconds
}

// Returns the current time in epoch seconds.
func nowEpochSecs() int64 {
	return time.Now().UnixMilli() / 1000
}

func (a *AccessToken) Load(r io.Reader) error {
	if err := json.NewDecoder(r).Decode(a); err != nil {
		return err
	}
	a.CreatedOn = nowEpochSecs()
	return nil
}

func (a *AccessToken) String() string {
	return a.Token
}

// Instant the token expires in epoch seconds.
func (a *AccessToken) ExpiresOn() int64 {
	return a.CreatedOn + int64(a.ExpiresIn)
}

// Anticipate access token expiration by 60 seconds
func (a *AccessToken) IsExpired() bool {
	return nowEpochSecs() > a.ExpiresOn()-60
}

type ClientCredentials struct {
	ClientID             string `json:"clientId"`
	ClientSecret         string `json:"-"`
	ClientCredentialsUrl string `json:"clientCredentialsUrl"`
	Audience             string `json:"audience"`
}
