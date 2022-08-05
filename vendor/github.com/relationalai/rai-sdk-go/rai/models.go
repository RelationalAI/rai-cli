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
	"time"

	"github.com/relationalai/rai-sdk-go/protos/generated"
)

// REST API v1

//
// Resources
//

type Database struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Region      string `json:"region"`
	AccountName string `json:"account_name"`
	CreatedBy   string `json:"created_by"`
	CreatedOn   string `json:"created_on"`
	DeletedBy   string `json:"deleted_by,omitempty"`
	DeletedOn   string `json:"deleted_on,omitempty"`
	State       string `json:"state"`
}

type EDB struct {
	Name   string   `json:"name"`
	Keys   []string `json:"keys"`
	Values []string `json:"values"`
}

type Engine struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Region      string `json:"region"`
	AccountName string `json:"account_name"`
	CreatedBy   string `json:"created_by"`
	CreatedOn   string `json:"created_on,omitempty"` // todo: required?
	DeletedOn   string `json:"deleted_on,omitempty"`
	Size        string `json:"size"`
	State       string `json:"state"`
}

type Model struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type OAuthClient struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	AccountName string    `json:"account_name"`
	CreatedBy   string    `json:"created_by"`
	CreatedOn   time.Time `json:"created_on"`
}

type OAuthClientExtra struct {
	OAuthClient
	Permissions []string `json:"permissions"`
	Secret      string   `json:"secret"`
}

type User struct {
	AccountName string   `json:"account_name"`
	Email       string   `json:"email"`
	ID          string   `json:"id"`
	IDProviers  []string `json:"id_providers"`
	Roles       []string `json:"roles"`
	Status      string   `json:"status"`
}

//
// Transaction results
//

type ClientProblem struct {
	Type        string `json:"type"`
	ErrorCode   string `json:"error_code"`
	IsError     bool   `json:"is_error"`
	IsException bool   `json:"is_exception"`
	Message     string `json:"message"`
	Report      string `json:"report"`
}

type RelKey struct {
	Name   string   `json:"name"`
	Keys   []string `json:"keys"`
	Values []string `json:"values"`
}

type Relation struct {
	RelKey  RelKey          `json:"rel_key"`
	Columns [][]interface{} `json:"columns"`
}

type TransactionResult struct {
	Aborted  bool            `json:"aborted"`
	Output   []Relation      `json:"output"`
	Problems []ClientProblem `json:"problems"`
}

//
// Request/response payloads
//

type createDatabaseRequest struct {
	Name   string `json:"name"`
	Source string `json:"source_name"`
}

type createDatabaseResponse struct {
	Database Database `json:"database"`
}

type createEngineRequest struct {
	Name   string `json:"name"`
	Size   string `json:"size"`
	Region string `json:"region"` // todo: isnt region part of the context?
}

type createEngineResponse struct {
	Engine Engine `json:"compute"`
}

type createOAuthClientRequest struct {
	Name        string   `json:"name"`
	Permissions []string `json:"permissions"`
}

type createOAuthClientResponse struct {
	Client OAuthClientExtra `json:"client"`
}

type getOAuthClientResponse struct {
	createOAuthClientResponse
}

type createUserRequest struct {
	Email string   `json:"email"`
	Roles []string `json:"roles"`
}

type createUserResponse struct {
	User User `json:"user"`
}

type deleteDatabaseRequest struct {
	Name string `json:"name"`
}

type deleteDatabaseResponse struct {
	Name    string `json:"name"`
	Message string `json:"message"`
}

type deleteEngineRequest struct {
	Name string `json:"name"`
}

type deleteEngineResponse struct {
	Status DeleteEngineStatus `json:"status"`
}

type DeleteEngineStatus struct {
	Name    string `json:"name"`
	State   string `json:"state"`
	Message string `json:"message"`
}

type DeleteOAuthClientResponse struct {
	ID      string `json:"client_id"`
	Message string `json:"message"`
}

type getDatabaseResponse struct {
	Databases []Database `json:"databases"`
}

type getEngineResponse struct {
	Engines []Engine `json:"computes"`
}

type getUserResponse struct {
	User User `json:"user"`
}

type listDatabasesResponse struct {
	Databases []Database `json:"databases"`
}

type listEDBsResponse struct {
	Actions []struct {
		Result struct {
			Rels []EDB `json:"rels"`
		} `json:"result"`
	} `json:"actions"`
}

type listEnginesResponse struct {
	Engines []Engine `json:"computes"`
}

type listOAuthClientsResponse struct {
	Clients []OAuthClient `json:"clients"`
}

type listModelsResponse struct {
	Actions []struct {
		Result struct {
			Models []Model `json:"sources"`
		} `json:"result"`
	} `json:"actions"`
}

type DeleteUserResponse struct {
	ID      string `json:"user_id"`
	Message string `json:"message"`
}

type listUsersResponse struct {
	Users []User `json:"users"`
}

type UpdateUserRequest struct {
	Status string   `json:"status,omitempty"`
	Roles  []string `json:"roles,omitempty"`
}

type updateUserResponse struct {
	User User `json:"user"`
}

//
// Transaction async models
//

type TransactionAsyncFile struct {
	Name        string
	Filename    string
	ContentType string
	Data        []byte
}

type ArrowRelation struct {
	RelationID string
	Table      []interface{}
}

type TransactionAsyncSingleResponse struct {
	Transaction TransactionAsyncResponse `json:"transaction"`
}

type TransactionAsyncMultipleResponses struct {
	Transactions []TransactionAsyncResponse `json:"transactions"`
}

type TransactionAsyncResponse struct {
	ID                    string `json:"id"`
	State                 string `json:"state"`
	AccountName           string `json:"account_name,omitempty"`
	CreatedBy             string `json:"created_by,omitempty"`
	CreatedOn             int64  `json:"created_on,omitempty"`
	FinishedAt            int64  `json:"finished_at,omitempty"`
	DatabaseName          string `json:"database_name,omitempty"`
	ReadOnly              bool   `json:"read_only,omitempty"`
	UserAgent             string `json:"user_agent,omitempty"`
	Query                 string `json:"query,omitempty"`
	LastRequestedInterval int64  `json:"last_requested_interval,omitempty"`
}

type TransactionAsyncCancelResponse struct {
	Message string `json:"message"`
}

type IntegrityConstraintViolation struct {
	Type    string   `json:"type"`
	Sources []Source `json:"sources"`
}

type Source struct {
	RelKey RelKey `json:"rel_key"`
	Source string `json:"source"`
	Type   string `json:"type"`
}

type TransactionAsyncResult struct {
	// If !GotCompleteResult, keep polling until Transaction reaches terminal State.
	GotCompleteResult bool
	Transaction       TransactionAsyncResponse
	Results           []ArrowRelation
	Metadata          generated.MetadataInfo
	Problems          []interface{}
}
