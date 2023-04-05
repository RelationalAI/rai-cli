// Copyright 2022-2023 RelationalAI, Inc.
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

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/relationalai/rai-sdk-go/rai/pb"
)

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
// Transaction v1 (deprecated)
//

type DbAction map[string]interface{}

// The transaction "request" envelope
type TransactionV1 struct {
	Region        string
	Database      string
	Engine        string
	Mode          string
	Source        string
	Abort         bool
	Readonly      bool
	NoWaitDurable bool
	Version       int
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

type ProblemV1 struct {
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

type RelationV1 struct {
	RelKey  RelKey          `json:"rel_key"`
	Columns [][]interface{} `json:"columns"`
}

type TransactionResult struct {
	Aborted  bool         `json:"aborted"`
	Output   []RelationV1 `json:"output"`
	Problems []ProblemV1  `json:"problems"`
}

//
// Transaction
//

type TransactionState string

const (
	Created   TransactionState = "CREATED" // Created, queued for execution
	Running   TransactionState = "RUNNING"
	Completed TransactionState = "COMPLETED"
	Aborted   TransactionState = "ABORTED"
)

type Transaction struct {
	ID                    string           `json:"id"`
	AccountName           string           `json:"account_name,omitempty"`
	Database              string           `json:"database_name,omitempty"`
	Query                 string           `json:"query,omitempty"`
	State                 TransactionState `json:"state"`
	ReadOnly              bool             `json:"read_only,omitempty"`
	CreatedBy             string           `json:"created_by,omitempty"`
	CreatedOn             int64            `json:"created_on,omitempty"`
	FinishedAt            int64            `json:"finished_at,omitempty"`
	LastRequestedInterval int64            `json:"last_requested_interval,omitempty"`
}

type TransactionRequest struct {
	Database string   `json:"dbname"`
	Engine   string   `json:"engine_name"`
	Query    string   `json:"query"`
	ReadOnly bool     `json:"readonly"`
	Inputs   []any    `json:"v1_inputs"`
	Tags     []string `json:"tags"`
}

type Problem struct {
	Type        string `json:"type"`
	ErrorCode   string `json:"error_code"`
	Message     string `json:"message"`
	Report      string `json:"report"`
	Path        string `json:"path"` // ?
	IsError     bool   `json:"is_error"`
	IsException bool   `json:"is_exception"`
}

type Signature []any

type TransactionMetadata struct {
	Info   *pb.MetadataInfo     // protobuf metadata
	sigMap map[string]Signature // id => metadata signature
}

type Partition struct {
	record arrow.Record
	cols   []Column
}

type TransactionResponse struct {
	Transaction Transaction
	Metadata    *TransactionMetadata
	Partitions  map[string]*Partition
	Problems    []Problem // todo: move to relational rep
	relations   RelationCollection
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
// Integrations
//

type Integration struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Account    string `json:"account"`
	CreatedBy  string `json:"createdBy"`
	CreatedOn  string `json:"createdOn"`
	State      string `json:"state"`
	ConsentURL string `json:"consentUrl"`
	Snowflake  struct {
		Account string `json:"account"`
	} `json:"snowflake"`
	RAI struct {
		Engine string `json:"engine"`
	} `json:"rai"`
}

type SnowflakeCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type createSnowflakeIntegrationRequest struct {
	Name      string `json:"name"`
	Snowflake struct {
		Account string               `json:"account"` // snowflake account name
		Admin   SnowflakeCredentials `json:"admin"`   // not-persisted
		Proxy   SnowflakeCredentials `json:"proxy"`   // persisted
	} `json:"snowflake"`
	RAI struct {
		Engine string `json:"engine"`
	} `json:"rai"`
}

type deleteSnowflakeIntegrationRequest struct {
	Snowflake struct {
		Admin SnowflakeCredentials `json:"admin"` // not-persisted
	} `json:"snowflake"`
}

//
// Snowflake Database Links
//

type SnowflakeDatabaseLink struct {
	ID          string `json:"id"`
	Name        string `json:"name"` // database.schema
	Account     string `json:"account"`
	CreatedBy   string `json:"createdBy"`
	CreatedOn   string `json:"createdOn"`
	State       string `json:"state"`
	Integration string `json:"integration"`
	Snowflake   struct {
		Database string `json:"database"`
		Schema   string `json:"schema"`
	} `json:"snowflake"`
}

type createSnowflakeDatabaseLinkRequest struct {
	Snowflake struct {
		Database    string               `json:"database"`
		Schema      string               `json:"schema"`
		Role        string               `json:"role"`
		Credentials SnowflakeCredentials `json:"credentials"` // not-persisted
	} `json:"snowflake"`
}

type deleteSnowflakeDatabaseLinkRequest struct {
	Snowflake struct {
		Role        string               `json:"role"`
		Credentials SnowflakeCredentials `json:"credentials"` // not-persisted
	} `json:"snowflake"`
}

//
// Snowflake Data Stream
//

type SnowflakeDataStream struct {
	Account     string `json:"account"` // partition key
	ID          string `json:"id"`
	Name        string `json:"name"` // database.schema.object
	Integration string `json:"integration"`
	DbLink      string `json:"dbLink"`
	CreatedBy   string `json:"createdBy"`
	CreatedOn   string `json:"createdOn"`
	State       string `json:"state"`
	Snowflake   struct {
		Database string `json:"database"`
		Schema   string `json:"schema"`
		Object   string `json:"object"` // fully qualified object name
	} `json:"snowflake"`
	RAI struct {
		Database string `json:"database"`
		Relation string `json:"relation"`
	} `json:"rai"`
}

type createSnowflakeDataStreamRequest struct {
	Snowflake struct {
		Object      string               `json:"object"` // fully qualified object name
		IsView      bool                 `json:"isView"`
		Role        string               `json:"role"`
		Warehouse   string               `json:"warehouse"`
		Credentials SnowflakeCredentials `json:"credentials"` // not-persisted
	} `json:"snowflake"`
	RAI struct {
		Database string `json:"database"`
		Relation string `json:"relation"`
	} `json:"rai"`
}

type deleteSnowflakeDataStreamRequest struct {
	Snowflake struct {
		Role        string               `json:"role"`
		Credentials SnowflakeCredentials `json:"credentials"` // not-persisted
	} `json:"snowflake"`
}
