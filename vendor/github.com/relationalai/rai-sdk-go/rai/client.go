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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v7/arrow/ipc"
	"github.com/pkg/errors"
	"github.com/relationalai/rai-sdk-go/rai/pb"
	"google.golang.org/protobuf/proto"
)

const userAgent = "rai-sdk-go/" + Version

type PreRequestHook func(*http.Request) *http.Request

type ClientOptions struct {
	Config
	HTTPClient         *http.Client
	AccessTokenHandler AccessTokenHandler
	PreRequestHook     PreRequestHook
}

func NewClientOptions(cfg *Config) *ClientOptions {
	return &ClientOptions{Config: *cfg}
}

type Client struct {
	ctx                context.Context
	Region             string
	Scheme             string
	Host               string
	Port               string
	HttpClient         *http.Client
	accessTokenHandler AccessTokenHandler
	preRequestHook     PreRequestHook
}

const DefaultHost = "azure.relationalai.com"
const DefaultPort = "443"
const DefaultRegion = "us-east"
const DefaultScheme = "https"

func NewClient(ctx context.Context, opts *ClientOptions) *Client {
	if opts == nil {
		opts = &ClientOptions{}
	}
	host := opts.Host
	if host == "" {
		host = DefaultHost
	}
	port := opts.Port
	if port == "" {
		port = DefaultPort
	}
	region := opts.Region
	if region == "" {
		region = DefaultRegion
	}
	scheme := opts.Scheme
	if scheme == "" {
		scheme = DefaultScheme
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{}
	}
	client := &Client{
		ctx:            ctx,
		Region:         region,
		Scheme:         scheme,
		Host:           host,
		Port:           port,
		preRequestHook: opts.PreRequestHook,
		HttpClient:     opts.HTTPClient}
	if opts.AccessTokenHandler != nil {
		client.accessTokenHandler = opts.AccessTokenHandler
	} else if opts.Credentials == nil {
		client.accessTokenHandler = NewNopAccessTokenHandler()
	} else {
		client.accessTokenHandler = NewClientCredentialsHandler(client, opts.Credentials)
	}
	return client
}

// Returns a new client using the background context and config settings from
// the named profile.
func NewClientFromConfig(profile string) (*Client, error) {
	var cfg Config
	if err := LoadConfigProfile(profile, &cfg); err != nil {
		return nil, err
	}

	opts := ClientOptions{Config: cfg}
	return NewClient(context.Background(), &opts), nil
}

// Returns a new client using the background context and config settings from
// the default profile.
func NewDefaultClient() (*Client, error) {
	return NewClientFromConfig(DefaultConfigProfile)
}

func (c *Client) Context() context.Context {
	return c.ctx
}

func (c *Client) SetContext(ctx context.Context) {
	c.ctx = ctx
}

func (c *Client) SetAccessTokenHandler(handler AccessTokenHandler) {
	c.accessTokenHandler = handler
}

// Ensures that the given path is a fully qualified URL.
func (c *Client) ensureUrl(path string) string {
	if len(path) > 0 && path[0] == '/' {
		return c.Url(path)
	}
	return path // assume its a URL
}

// Returns a URL constructed from given path.
func (c *Client) Url(path string) string {
	return fmt.Sprintf("%s://%s:%s%s", c.Scheme, c.Host, c.Port, path)
}

/* #nosec */
const getAccessTokenBody = `{
	"client_id": "%s",
	"client_secret": "%s",
	"audience": "%s",
	"grant_type": "client_credentials"
}`

// Returns the current access token
func (c *Client) AccessToken() (string, error) {
	return c.accessTokenHandler.GetAccessToken()
}

// Fetch a new access token using the given client credentials.
func (c *Client) GetAccessToken(creds *ClientCredentials) (*AccessToken, error) {
	audience := creds.Audience
	body := fmt.Sprintf(getAccessTokenBody, creds.ClientID, creds.ClientSecret, audience)
	req, err := http.NewRequest(http.MethodPost, creds.ClientCredentialsUrl, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req = req.WithContext(c.ctx)
	c.ensureHeaders(req, nil)
	rsp, err := c.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()
	token := &AccessToken{}
	if err = token.Load(rsp.Body); err != nil {
		return nil, err
	}
	return token, nil
}

// Authenticate the given request using the configured credentials.
func (c *Client) authenticate(req *http.Request) error {
	token, err := c.AccessToken()
	if err != nil || token == "" {
		return err // don't authenticate the request
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	return nil
}

// Add any missing headers to the given request.
func (c *Client) ensureHeaders(req *http.Request, headers map[string]string) {
	if v := req.Header.Get("accept"); v == "" {
		req.Header.Set("Accept", "application/json")
	}
	if v := req.Header.Get("content-type"); v == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if v := req.Header.Get("user-agent"); v == "" {
		req.Header.Set("User-Agent", userAgent)
	}

	// add extra headers
	for h, v := range headers {
		req.Header.Set(h, v)
	}
}

func (c *Client) newRequest(method, path string, args url.Values, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, c.ensureUrl(path), body)
	if err != nil {
		return nil, err
	}
	if len(args) > 0 {
		req.URL.RawQuery = args.Encode()
	}
	return req, nil
}

func (c *Client) Delete(path string, args url.Values, data, result interface{}) error {
	return c.request(http.MethodDelete, path, nil, args, data, result)
}

func (c *Client) Get(path string, headers map[string]string, args url.Values, result interface{}) error {
	return c.request(http.MethodGet, path, headers, args, nil, result)
}

func (c *Client) Patch(path string, args url.Values, data, result interface{}) error {
	return c.request(http.MethodPatch, path, nil, args, data, result)
}

func (c *Client) Post(path string, args url.Values, data, result interface{}) error {
	return c.request(http.MethodPost, path, nil, args, data, result)
}

func (c *Client) Put(path string, args url.Values, data, result interface{}) error {
	return c.request(http.MethodPut, path, nil, args, data, result)
}

// Marshal the given item as a JSON string and return an io.Reader.
func marshal(item interface{}) (io.Reader, error) {
	if item == nil {
		return nil, nil
	}
	data, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}
	return strings.NewReader(string(data)), nil
}

// Unmarshal the JSON object from the given response body.
func unmarshal(rsp *http.Response, result interface{}) error {
	if result == nil {
		return nil
	}
	data, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	err = json.Unmarshal(data, result)
	if err != nil {
		return err
	}
	return nil
}

// Construct request, execute and unmarshal response.
func (c *Client) request(
	method, path string, headers map[string]string, args url.Values, data, result interface{},
) error {
	body, err := marshal(data)
	if err != nil {
		return err
	}
	req, err := c.newRequest(method, path, args, body)
	if err != nil {
		return err
	}
	c.ensureHeaders(req, headers)
	if err := c.authenticate(req); err != nil {
		return err
	}
	rsp, err := c.Do(req)
	if err != nil {
		return err
	}
	switch out := result.(type) {
	case **http.Response:
		*out = rsp // caller will handle response
		return nil
	}
	defer rsp.Body.Close()
	return unmarshal(rsp, result)
}

type HTTPError struct {
	StatusCode int
	Headers    http.Header
	Body       string
}

func (e HTTPError) Error() string {
	statusText := http.StatusText(e.StatusCode)
	if e.Body != "" {
		return fmt.Sprintf("%d %s %s\n%s", e.StatusCode, e.Headers, statusText, e.Body)
	}
	return fmt.Sprintf("%d %s %s", e.StatusCode, e.Headers, statusText)
}

func newHTTPError(status int, headers http.Header, body string) error {
	return HTTPError{StatusCode: status, Headers: headers, Body: body}
}

var ErrNotFound = newHTTPError(http.StatusNotFound, nil, "")

// Returns an HTTPError corresponding to the given response.
func httpError(rsp *http.Response) error {
	// assert rsp.Status < 200 || rsp.Status > 299
	data, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		data = []byte{}
	}
	return newHTTPError(rsp.StatusCode, rsp.Header, string(data))
}

// Ansers if the given response has a status code representing an error.
func isErrorStatus(rsp *http.Response) bool {
	return rsp.StatusCode < 200 || rsp.StatusCode > 299
}

// Execute the given request and return the response or error.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	req = req.WithContext(c.ctx)
	if c.preRequestHook != nil {
		req = c.preRequestHook(req)
	}
	rsp, err := c.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if isErrorStatus(rsp) {
		defer rsp.Body.Close()
		return nil, httpError(rsp)
	}
	return rsp, nil
}

//
// RAI APIs
//

const (
	PathDatabase     = "/database"
	PathEngine       = "/compute"
	PathIntegrations = "/integration/v1alpha1/integrations"
	PathOAuthClients = "/oauth-clients"
	PathTransaction  = "/transaction"
	PathTransactions = "/transactions"
	PathUsers        = "/users"
)

func makePath(parts ...string) string {
	return strings.Join(parts, "/")
}

// Add the filter to the given query args.
func addFilter(args url.Values, name string, value interface{}) error {
	if value == nil {
		return nil // ignore
	}
	switch v := value.(type) {
	case int:
		args.Add(name, strconv.Itoa(v))
	case string:
		args.Add(name, v)
	case []string:
		for _, item := range v {
			args.Add(name, item)
		}
	default:
		return errors.Errorf("bad filter value '%v'", v)
	}
	return nil
}

// Add the contents of the filter map to the given query args.
func addFilterMap(args url.Values, m map[string]interface{}) error {
	for k, v := range m {
		if v == nil {
			continue // ignore
		}
		switch vv := v.(type) {
		case int:
			args.Add(k, strconv.Itoa(vv))
		case string:
			args.Add(k, vv)
		case []string:
			for _, item := range vv {
				if item == "" {
					continue // ignore
				}
				args.Add(k, item)
			}
		default:
			return errors.Errorf("bad filter value '%v'", vv)
		}
	}
	return nil
}

var ErrMissingFilterValue = errors.New("missing filter value")

// Construct a url.Values struct from the given filters.
func queryArgs(filters ...interface{}) (url.Values, error) {
	args := url.Values{}
	for i := 0; i < len(filters); i++ {
		filter := filters[i]
		switch item := filter.(type) {
		case map[string]interface{}:
			if err := addFilterMap(args, item); err != nil {
				return nil, err
			}
		case string:
			if i == len(filters)-1 {
				return nil, ErrMissingFilterValue
			}
			i++
			value := filters[i]
			if err := addFilter(args, item, value); err != nil {
				return nil, err
			}
		default:
			return nil, errors.Errorf("bad filter arg '%v'", item)
		}
	}
	return args, nil
}

//
// Databases
//

func (c *Client) CloneDatabase(database, source string) (*Database, error) {
	var result createDatabaseResponse
	data := &createDatabaseRequest{Name: database, Source: source}
	err := c.Put(PathDatabase, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return &result.Database, nil
}

func (c *Client) CreateDatabase(database string) (*Database, error) {
	var result createDatabaseResponse
	data := &createDatabaseRequest{Name: database}
	err := c.Put(PathDatabase, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return &result.Database, nil
}

func (c *Client) DeleteDatabase(database string) error {
	var result deleteDatabaseResponse
	data := &deleteDatabaseRequest{Name: database}
	return c.Delete(PathDatabase, nil, data, &result)
}

func (c *Client) GetDatabase(database string) (*Database, error) {
	args, err := queryArgs("name", database)
	if err != nil {
		return nil, err
	}
	var result getDatabaseResponse
	err = c.Get(PathDatabase, nil, args, &result)
	if err != nil {
		return nil, err
	}
	if len(result.Databases) == 0 {
		return nil, ErrNotFound
	}
	return &result.Databases[0], nil
}

func (c *Client) ListDatabases(filters ...interface{}) ([]Database, error) {
	args, err := queryArgs(filters...)
	if err != nil {
		return nil, err
	}
	var result listDatabasesResponse
	err = c.Get(PathDatabase, nil, args, &result)
	if err != nil {
		return nil, err
	}
	return result.Databases, nil
}

//
// Engines
//

// Answeres if the given state is a terminal state.
func isTerminalState(state, targetState string) bool {
	return state == targetState || strings.Contains(state, "FAILED")
}

// Request the creation of an engine, and wait for the opeartion to complete.
// This can block the caller for up to a minute.
func (c *Client) CreateEngine(engine, size string) (*Engine, error) {
	rsp, err := c.CreateEngineAsync(engine, size)
	if err != nil {
		return nil, err
	}
	for !isTerminalState(rsp.State, "PROVISIONED") {
		time.Sleep(5 * time.Second)
		if rsp, err = c.GetEngine(engine); err != nil {
			return nil, err
		}
	}
	return rsp, nil
}

// Request the creation of an engine, and immediately return. The process
// of provisioning a new engine can take up to a minute.
func (c *Client) CreateEngineAsync(engine, size string) (*Engine, error) {
	var result createEngineResponse
	data := &createEngineRequest{Region: c.Region, Name: engine, Size: size}
	err := c.Put(PathEngine, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return &result.Engine, nil
}

// Request the deletion of an engine and wait for the operation to complete.
func (c *Client) DeleteEngine(engine string) error {
	rsp, err := c.DeleteEngineAsync(engine)
	if err != nil {
		return err
	}
	for !isTerminalState(rsp.State, "DELETED") {
		time.Sleep(3 * time.Second)
		if rsp, err = c.GetEngine(engine); err != nil {
			if e, ok := err.(HTTPError); ok {
				if e.StatusCode == ErrNotFound.(HTTPError).StatusCode {
					return nil // successfully deleted
				}
			}
			return err
		}
	}
	return nil
}

func (c *Client) DeleteEngineAsync(engine string) (*Engine, error) {
	var result deleteEngineResponse
	data := &deleteEngineRequest{Name: engine}
	err := c.Delete(PathEngine, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return c.GetEngine(engine) // normalize return type
}

func (c *Client) GetEngine(engine string) (*Engine, error) {
	args, err := queryArgs("name", engine, "deleted_on", "")
	if err != nil {
		return nil, err
	}
	var result getEngineResponse
	err = c.Get(PathEngine, nil, args, &result)
	if err != nil {
		return nil, err
	}
	if len(result.Engines) == 0 {
		return nil, ErrNotFound
	}
	return &result.Engines[0], nil
}

func (c *Client) ListEngines(filters ...interface{}) ([]Engine, error) {
	args, err := queryArgs(filters...)
	if err != nil {
		return nil, err
	}
	var result listEnginesResponse
	err = c.Get(PathEngine, nil, args, &result)
	if err != nil {
		return nil, err
	}
	return result.Engines, nil
}

//
// OAuth Clients
//

func (c *Client) CreateOAuthClient(
	name string, perms []string,
) (*OAuthClientExtra, error) {
	var result createOAuthClientResponse
	data := createOAuthClientRequest{Name: name, Permissions: perms}
	err := c.Post(PathOAuthClients, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return &result.Client, nil
}

func (c *Client) DeleteOAuthClient(id string) (*DeleteOAuthClientResponse, error) {
	var result DeleteOAuthClientResponse
	err := c.Delete(makePath(PathOAuthClients, id), nil, nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// Returns the OAuth client with the given name or ErrNotFound if it does not exist.
func (c *Client) FindOAuthClient(name string) (*OAuthClient, error) {
	clients, err := c.ListOAuthClients()
	if err != nil {
		return nil, err
	}
	for _, client := range clients {
		if client.Name == name {
			return &client, nil
		}
	}
	return nil, ErrNotFound
}

func (c *Client) GetOAuthClient(id string) (*OAuthClientExtra, error) {
	var result getOAuthClientResponse
	err := c.Get(makePath(PathOAuthClients, id), nil, nil, &result)
	if err != nil {
		return nil, err
	}
	return &result.Client, nil
}

func (c *Client) ListOAuthClients() ([]OAuthClient, error) {
	var result listOAuthClientsResponse
	err := c.Get(PathOAuthClients, nil, nil, &result)
	if err != nil {
		return nil, err
	}
	return result.Clients, nil
}

//
// Models
//

func (c *Client) DeleteModel(
	database, engine, name string,
) (*TransactionResult, error) {
	return c.DeleteModels(database, engine, []string{name})
}

func (c *Client) DeleteModels(
	database, engine string, models []string,
) (*TransactionResult, error) {
	var result TransactionResult
	tx := TransactionV1{
		Region:   c.Region,
		Database: database,
		Engine:   engine,
		Mode:     "OPEN",
		Readonly: false}
	data := tx.Payload(makeDeleteModelsAction(models))
	err := c.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	return &result, err
}

func (c *Client) GetModel(database, engine, model string) (*Model, error) {
	var result listModelsResponse
	tx := NewTransaction(c.Region, database, engine, "OPEN")
	data := tx.Payload(makeListModelsAction())
	err := c.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	// assert len(result.Actions) == 1
	for _, item := range result.Actions[0].Result.Models {
		if item.Name == model {
			return &item, nil
		}
	}
	return nil, ErrNotFound
}

func (c *Client) LoadModel(
	database, engine, name string, r io.Reader,
) (*TransactionResult, error) {
	return c.LoadModels(database, engine, map[string]io.Reader{name: r})
}

func (c *Client) LoadModels(
	database, engine string, models map[string]io.Reader,
) (*TransactionResult, error) {
	var result TransactionResult
	tx := TransactionV1{
		Region:   c.Region,
		Database: database,
		Engine:   engine,
		Mode:     "OPEN",
		Readonly: false}
	actions := []DbAction{}
	for name, r := range models {
		model, err := ioutil.ReadAll(r)
		if err != nil {
			return nil, err
		}
		action := makeLoadModelAction(name, string(model))
		actions = append(actions, action)
	}
	data := tx.Payload(actions...)
	err := c.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// Returns a list of model names for the given database.
func (c *Client) ListModelNames(database, engine string) ([]string, error) {
	var models listModelsResponse
	tx := NewTransaction(c.Region, database, engine, "OPEN")
	data := tx.Payload(makeListModelsAction())
	err := c.Post(PathTransaction, tx.QueryArgs(), data, &models)
	if err != nil {
		return nil, err
	}
	actions := models.Actions
	// assert len(actions) == 1
	result := []string{}
	for _, model := range actions[0].Result.Models {
		result = append(result, model.Name)
	}
	return result, nil
}

// Returns the names of models installed in the given database.
func (c *Client) ListModels(database, engine string) ([]Model, error) {
	var models listModelsResponse
	tx := NewTransaction(c.Region, database, engine, "OPEN")
	data := tx.Payload(makeListModelsAction())
	err := c.Post(PathTransaction, tx.QueryArgs(), data, &models)
	if err != nil {
		return nil, err
	}
	actions := models.Actions
	// assert len(actions) == 1
	return actions[0].Result.Models, nil
}

//
// Transactions v1 (deprecated)
//

func NewTransaction(region, database, engine, mode string) *TransactionV1 {
	return &TransactionV1{
		Region:   region,
		Database: database,
		Engine:   engine,
		Mode:     mode}
}

// Constructs a transaction request payload.
func (tx *TransactionV1) Payload(actions ...DbAction) map[string]interface{} {
	data := map[string]interface{}{
		"type":           "Transaction",
		"abort":          tx.Abort,
		"actions":        makeActions(actions...),
		"dbname":         tx.Database,
		"nowait_durable": tx.NoWaitDurable,
		"readonly":       tx.Readonly,
		"version":        tx.Version}
	if tx.Engine != "" {
		data["computeName"] = tx.Engine
	}
	if tx.Source != "" {
		data["source_dbname"] = tx.Source
	}
	if tx.Mode != "" {
		data["mode"] = tx.Mode
	} else {
		data["mode"] = "OPEN"
	}
	return data
}

func (tx *TransactionV1) QueryArgs() url.Values {
	result := url.Values{}
	result.Add("dbname", tx.Database)
	result.Add("compute_name", tx.Engine)
	result.Add("open_mode", tx.Mode)
	result.Add("region", tx.Region)
	if tx.Source != "" {
		result.Add("source_dbname", tx.Source)
	}
	return result
}

// Wrap each of the given actions in a LabeledAction.
func makeActions(actions ...DbAction) []DbAction {
	result := []DbAction{}
	for i, action := range actions {
		item := map[string]interface{}{
			"name":   fmt.Sprintf("action%d", i),
			"type":   "LabeledAction",
			"action": action}
		result = append(result, item)
	}
	return result
}

func makeRelKey(name, key string) map[string]interface{} {
	return map[string]interface{}{
		"type":   "RelKey",
		"name":   name,
		"keys":   []string{key},
		"values": []string{}}
}

func reltype(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return "RAI_VariableSizeStrings.VariableSizeString", nil
	default:
		return "", errors.Errorf("bad query input type: '%T'", v)
	}
}

func makeQuerySource(name, model string) map[string]interface{} {
	return map[string]interface{}{
		"type":  "Source",
		"name":  name,
		"path":  "",
		"value": model}
}

func makeDeleteModelsAction(models []string) DbAction {
	return DbAction{"type": "ModifyWorkspaceAction", "delete_source": models}
}

func makeLoadModelAction(name, model string) DbAction {
	return DbAction{
		"type":    "InstallAction",
		"sources": []map[string]interface{}{makeQuerySource(name, model)}}
}

func makeListModelsAction() DbAction {
	return DbAction{"type": "ListSourceAction"}
}

func makeListEDBAction() DbAction {
	return DbAction{"type": "ListEdbAction"}
}

func makeQueryAction(source string, inputs map[string]string) (DbAction, error) {
	actionInputs := []map[string]interface{}{}
	for k, v := range inputs {
		actionInput, err := makeQueryActionInput(k, v)
		if err != nil {
			return nil, err
		}
		actionInputs = append(actionInputs, actionInput)
	}
	result := map[string]interface{}{
		"type":    "QueryAction",
		"source":  makeQuerySource("query", source),
		"persist": []string{},
		"inputs":  actionInputs,
		"outputs": []string{}}
	return result, nil
}

func makeQueryActionInput(name, value string) (map[string]interface{}, error) {
	typename, err := reltype(value)
	if err != nil {
		return nil, err
	}
	result := map[string]interface{}{
		"type":    "Relation",
		"columns": [][]string{{value}},
		"rel_key": makeRelKey(name, typename)}
	return result, nil
}

// Deprecated: use `Execute`
func (c *Client) ExecuteV1(
	database, engine, source string,
	inputs map[string]string,
	readonly bool,
) (*TransactionResult, error) {
	var result TransactionResult
	tx := TransactionV1{
		Region:   c.Region,
		Database: database,
		Engine:   engine,
		Mode:     "OPEN",
		Readonly: readonly}
	queryAction, err := makeQueryAction(source, inputs)
	if err != nil {
		return nil, err
	}
	data := tx.Payload(queryAction)
	err = c.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

//
// Transactions
//

// Answers if the given transaction is in a terminal state.
func isTransactionComplete(tx *Transaction) bool {
	switch tx.State {
	case Completed, Aborted:
		return true
	}
	return false
}

const twoMinutes = 2 * time.Minute

// todo: consider making the polling coefficients part of tx options
func (c *Client) Execute(
	database, engine, source string,
	inputs map[string]string, readonly bool,
	tags ...string,
) (*TransactionResponse, error) {
	t0 := time.Now()
	rsp, err := c.ExecuteAsync(database, engine, source, inputs, readonly, tags...)
	if err != nil {
		return nil, err
	}
	if isTransactionComplete(&rsp.Transaction) {
		return rsp, nil // fast path
	}
	id := rsp.Transaction.ID
	opts := GetTransactionOptions{true, true, true}
	time.Sleep(500 * time.Millisecond)
	for {
		rsp, err := c.GetTransaction(id, opts)
		if err != nil {
			return nil, err
		}
		if isTransactionComplete(&rsp.Transaction) {
			return rsp, nil
		}
		delta := time.Since(t0)                  // total run time
		pause := time.Duration(int64(delta) / 5) // 20% of total run time
		if pause > twoMinutes {
			pause = twoMinutes
		}
		time.Sleep(pause)
	}
}

// Returns the results of a fast path response, which will contain data for
// the transaction resource, problems, metadata and results in various parts
// of the multipart response.
func ReadTransactionResponse(rsp *http.Response) (*TransactionResponse, error) {
	var result TransactionResponse

	h := rsp.Header.Get("content-type")
	ctype, params, err := mime.ParseMediaType(h)
	if err != nil {
		return nil, err
	}
	if ctype != "multipart/form-data" {
		return nil, fmt.Errorf("bad content type: '%s'", ctype)
	}
	r := multipart.NewReader(rsp.Body, params["boundary"])
	for {
		part, err := r.NextPart()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		switch part.FormName() {
		case "metadata":
			// ignore, deprecated

		case "metadata.proto":
			rsp, err := readTransactionMetadata(part)
			if err != nil {
				return nil, err
			}
			result.Metadata = rsp

		case "problems":
			rsp, err := readTransactionProblems(part)
			if err != nil {
				return nil, err
			}
			result.Problems = rsp

		case "relation-count":
			// ignore, this only exists to workaround a bug in some browsers
			// that panic on a multi-part response with zero parts.

		case "transaction":
			if err = readJSON(part, &result.Transaction); err != nil {
				return nil, err
			}

		default: // otherwise it's an errow encoded partition
			id, rsp, err := readTransactionPartition(part)
			if err != nil {
				return nil, err
			}
			if result.Partitions == nil {
				result.Partitions = map[string]*Partition{}
			}
			result.Partitions[id] = rsp
		}
	}
	return &result, nil
}

func (c *Client) ExecuteAsync(
	database, engine, query string,
	inputs map[string]string, readonly bool,
	tags ...string,
) (*TransactionResponse, error) {
	var inputList = make([]interface{}, 0)
	for k, v := range inputs {
		input, _ := makeQueryActionInput(k, v)
		inputList = append(inputList, input)
	}
	tx := TransactionRequest{
		Database: database,
		Engine:   engine,
		Query:    query,
		ReadOnly: readonly,
		Inputs:   inputList,
		Tags:     tags}
	var rsp *http.Response
	err := c.request(http.MethodPost, PathTransactions, nil, nil, tx, &rsp)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()
	if rsp.StatusCode == 200 {
		return ReadTransactionResponse(rsp) // fast path
	}
	if rsp.StatusCode != 201 {
		return nil, fmt.Errorf("unexpected status code '%d'", rsp.StatusCode)
	}
	var result TransactionResponse
	err = readJSON(rsp.Body, &result.Transaction)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// If any of the following are true, `GetTransaction` will retrieve the
// corresponding outputs, if available.
type GetTransactionOptions struct {
	Results  bool
	Metadata bool
	Problems bool
}

// Returns the transaction resource identified by `id` and any other outputs
// selected in `opts`, if available.
func (c *Client) GetTransaction(id string, opts ...GetTransactionOptions) (
	*TransactionResponse, error,
) {
	var result TransactionResponse
	rsp := struct{ Transaction *Transaction }{Transaction: &result.Transaction}
	err := c.Get(makePath(PathTransactions, id), nil, nil, &rsp)
	if err != nil {
		return nil, err
	}
	if !isTransactionComplete(&result.Transaction) {
		return &result, nil
	}

	var results, metadata, problems bool
	for _, opt := range opts {
		results = results || opt.Results
		metadata = metadata || opt.Metadata
		problems = problems || opt.Problems
	}
	var wg sync.WaitGroup
	var errR, errM, errP error
	if results {
		wg.Add(1)
		go func() {
			result.Partitions, errR = c.GetTransactionResults(id)
			wg.Done()
		}()
	}
	if metadata {
		wg.Add(1)
		go func() {
			result.Metadata, errM = c.GetTransactionMetadata(id)
			wg.Done()
		}()
	}
	if problems {
		wg.Add(1)
		go func() {
			result.Problems, errP = c.GetTransactionProblems(id)
			wg.Done()
		}()
	}
	wg.Wait()
	if results && errR != nil {
		return nil, errR
	}
	if metadata && errM != nil {
		return nil, errM
	}
	if problems && errP != nil {
		return nil, errP
	}
	return &result, nil // todo
}

func readJSON(r io.Reader, result interface{}) error {
	return json.NewDecoder(r).Decode(result)
}

func readTransactionMetadata(r io.Reader) (*TransactionMetadata, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var info pb.MetadataInfo
	err = proto.Unmarshal(data, &info)
	if err != nil {
		return nil, err
	}
	return &TransactionMetadata{Info: &info, sigMap: asSignatureMap(&info)}, nil
}

func (c *Client) GetTransactionMetadata(id string) (
	*TransactionMetadata, error,
) {
	var rsp *http.Response
	headers := map[string]string{"Accept": "application/x-protobuf"}
	err := c.Get(makePath(PathTransactions, id, "metadata"), headers, nil, &rsp)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()
	return readTransactionMetadata(rsp.Body)
}

func readTransactionProblems(r io.Reader) ([]Problem, error) {
	var result []Problem
	if err := readJSON(r, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// todo: deprecated, should be loaded from partitions
func (c *Client) GetTransactionProblems(id string) ([]Problem, error) {
	var result []Problem
	err := c.Get(makePath(PathTransactions, id, "problems"), nil, nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Read one partition from transactionr results.
func readTransactionPartition(part *multipart.Part) (string, *Partition, error) {
	h := part.Header.Get("content-type")
	ctype, _, err := mime.ParseMediaType(h)
	if err != nil {
		return "", nil, err
	}
	if ctype != "application/vnd.apache.arrow.stream" {
		return "", nil, fmt.Errorf("unknown content disposition '%s'", ctype)
	}
	r, err := ipc.NewReader(part)
	if err != nil {
		return "", nil, err
	}
	if r.Next() {
		id := part.FileName()
		record := r.Record()
		record.Retain()
		if r.Next() { // partitions are encoded in a single record
			return "", nil, errors.New("unexpected record in partition")
		}
		return id, newPartition(record), nil
	}
	return "", nil, errors.New("no records for partition")
}

// Read the results of `GetTransactionResults` which will contain a list of
// partitions in the parts of the multipart response.
func readTransactionResults(rsp *http.Response) (map[string]*Partition, error) {
	h := rsp.Header.Get("content-type")
	ctype, params, err := mime.ParseMediaType(h)
	if err != nil {
		return nil, err
	}
	if ctype != "multipart/form-data" {
		return nil, fmt.Errorf("bad content type: '%s'", ctype)
	}

	result := map[string]*Partition{}

	r := multipart.NewReader(rsp.Body, params["boundary"])
	for {
		part, err := r.NextPart()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		switch part.FormName() {
		case "relation-count": // ignore
		default:
			id, p, err := readTransactionPartition(part)
			if err != nil {
				return nil, err
			}
			result[id] = p
		}
	}
	return result, nil
}

func (c *Client) GetTransactionResults(id string) (map[string]*Partition, error) {
	var rsp *http.Response
	err := c.Get(makePath(PathTransactions, id, "results"), nil, nil, &rsp)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()
	return readTransactionResults(rsp)
}

type listTransactionsResponse struct {
	Transactions []Transaction `json:"transactions"`
}

func (c *Client) ListTransactions(tags ...string) ([]Transaction, error) {
	args, err := queryArgs("tags", tags)
	if err != nil {
		return nil, err
	}

	var result listTransactionsResponse
	err = c.Get(makePath(PathTransactions), nil, args, &result)
	return result.Transactions, err
}

type cancelTransactionResponse struct {
	Message string `json:"message"`
}

func (c *Client) CancelTransaction(id string) (string, error) {
	var result cancelTransactionResponse
	if err := c.Post(makePath(PathTransactions, id, "cancel"), nil, nil, &result); err != nil {
		return "", err
	}
	return result.Message, nil
}

// TransactionResponse

func (t *TransactionResponse) EnsureMetadata(c *Client) (*TransactionMetadata, error) {
	if t.Metadata == nil {
		metadata, err := c.GetTransactionMetadata(t.Transaction.ID)
		if err != nil {
			return nil, err
		}
		t.Metadata = metadata
	}
	return t.Metadata, nil
}

func (t *TransactionResponse) EnsureProblems(c *Client) ([]Problem, error) {
	if t.Problems == nil {
		problems, err := c.GetTransactionProblems(t.Transaction.ID)
		if err != nil {
			return nil, err
		}
		t.Problems = problems
	}
	return t.Problems, nil
}

func (t *TransactionResponse) EnsureResults(c *Client) (map[string]*Partition, error) {
	if t.Partitions == nil {
		partitions, err := c.GetTransactionResults(t.Transaction.ID)
		if err != nil {
			return nil, err
		}
		t.Partitions = partitions
	}
	return t.Partitions, nil
}

func (t *TransactionResponse) Partition(id string) *Partition {
	return t.Partitions[id]
}

func (t *TransactionResponse) Relation(id string) Relation {
	return newBaseRelation(t.Partitions[id], t.Signature(id))
}

// Answers if the given signature prefix matches the given signature, where
// the value "_" is a position wildcard.
func matchSig(pre, sig Signature) bool {
	if pre == nil {
		return true
	}
	if len(pre) > len(sig) {
		return false
	}
	for i, p := range pre {
		if p == "_" {
			continue
		}
		if p != sig[i] {
			return false
		}
	}
	return true
}

// Returns a collection of relations whose signature matches any of the
// optional prefix arguments, where value "_" in the prefix matches any value in the
// corresponding signature position.
func (t *TransactionResponse) Relations(args ...any) RelationCollection {
	if t.Metadata == nil {
		// cannot interpret partition data as without metadata
		return RelationCollection{}
	}
	if t.relations == nil {
		// construct collection of base relations
		c := RelationCollection{}
		for id, p := range t.Partitions {
			c = append(c, newBaseRelation(p, t.Signature(id)))
		}
		t.relations = c
	}
	return t.relations.Select(args...)
}

// Returns the type signature corresponding to the given relation ID.
func (t TransactionResponse) Signature(id string) Signature {
	return t.Metadata.Signature(id)
}

// Transaction based operations

func (c *Client) ListEDBs(database, engine string) ([]EDB, error) {
	var result listEDBsResponse
	tx := &TransactionV1{
		Region:   c.Region,
		Database: database,
		Engine:   engine,
		Mode:     "OPEN",
		Readonly: true}
	data := tx.Payload(makeListEDBAction())
	err := c.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}

	if len(result.Actions) == 0 {
		return []EDB{}, nil
	}

	// assert len(result.Actions) == 1
	return result.Actions[0].Result.Rels, nil
}

type CSVOptions struct {
	Schema     map[string]string
	HeaderRow  *int
	Delim      rune
	EscapeChar rune
	QuoteChar  rune
}

func NewCSVOptions() *CSVOptions {
	return &CSVOptions{}
}

func (opts *CSVOptions) WithDelim(delim rune) *CSVOptions {
	opts.Delim = delim
	return opts
}

func (opts *CSVOptions) WithEscapeChar(escapeChar rune) *CSVOptions {
	opts.EscapeChar = escapeChar
	return opts
}

func (opts *CSVOptions) WithQuoteChar(quoteChar rune) *CSVOptions {
	opts.QuoteChar = quoteChar
	return opts
}

func (opts *CSVOptions) WithHeaderRow(headerRow int) *CSVOptions {
	opts.HeaderRow = &headerRow
	return opts
}

func (opts *CSVOptions) WithSchema(schema map[string]string) *CSVOptions {
	opts.Schema = schema
	return opts
}

// Generates Rel schema config defs for the given CSV options.
func genSchemaConfig(b *strings.Builder, opts *CSVOptions) {
	if opts == nil {
		return
	}
	schema := opts.Schema
	if len(schema) == 0 {
		return
	}
	count := 0
	b.WriteString("def config:schema = ")
	for k, v := range schema {
		if count > 0 {
			b.WriteRune(';')
		}
		b.WriteString(fmt.Sprintf("\n    :%s, \"%s\"", k, v))
		count++
	}
	b.WriteRune('\n')
}

func genLiteralInt(v int) string {
	return strconv.Itoa(v)
}

func genLiteralRune(v rune) string {
	if v == '\'' {
		return "'\\''"
	}
	return fmt.Sprintf("'%s'", string(v))
}

// Returns a Rel literal for the given value.
func genLiteral(v interface{}) string {
	switch vv := v.(type) {
	case int:
		return genLiteralInt(vv)
	case rune:
		return genLiteralRune(vv)
	}
	panic("unreached")
}

// Generates a Rel syntax config def for the given option name and value.
func genSyntaxOption(b *strings.Builder, name string, value interface{}) {
	lit := genLiteral(value)
	def := fmt.Sprintf("def config:syntax:%s = %s\n", name, lit)
	b.WriteString(def)
}

// Generates Rel syntax config defs for the given CSV options.
func genSyntaxConfig(b *strings.Builder, opts *CSVOptions) {
	if opts == nil {
		return
	}
	if opts.HeaderRow != nil {
		genSyntaxOption(b, "header_row", *opts.HeaderRow)
	}
	if opts.Delim != 0 {
		genSyntaxOption(b, "delim", opts.Delim)
	}
	if opts.EscapeChar != 0 {
		genSyntaxOption(b, "escapechar", opts.EscapeChar)
	}
	if opts.QuoteChar != 0 {
		genSyntaxOption(b, "quotechar", opts.QuoteChar)
	}
}

// Generate Rel to load CSV data into a relation with the given name.
func genLoadCSV(relation string, opts *CSVOptions) string {
	b := new(strings.Builder)
	genSyntaxConfig(b, opts)
	genSchemaConfig(b, opts)
	b.WriteString("def config:data = data\n")
	b.WriteString(fmt.Sprintf("def insert:%s = load_csv[config]", relation))
	return b.String()
}

func (c *Client) LoadCSV(
	database, engine, relation string, r io.Reader, opts *CSVOptions,
) (*TransactionResult, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	source := genLoadCSV(relation, opts)
	inputs := map[string]string{"data": string(data)}
	return c.ExecuteV1(database, engine, source, inputs, false)
}

func (c *Client) LoadJSON(
	database, engine, relation string, r io.Reader,
) (*TransactionResult, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	b := new(strings.Builder)
	b.WriteString("def config:data = data\n")
	b.WriteString(fmt.Sprintf("def insert:%s = load_json[config]", relation))
	inputs := map[string]string{"data": string(data)}
	return c.ExecuteV1(database, engine, b.String(), inputs, false)
}

//
// Users
//

func (c *Client) CreateUser(email string, roles []string) (*User, error) {
	if len(roles) == 0 {
		roles = append(roles, "user")
	}
	var result createUserResponse
	data := &createUserRequest{Email: email, Roles: roles}
	err := c.Post(PathUsers, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return &result.User, nil
}

func (c *Client) DeleteUser(id string) (*DeleteUserResponse, error) {
	var result DeleteUserResponse
	err := c.Delete(makePath(PathUsers, id), nil, nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) DisableUser(id string) (*User, error) {
	req := UpdateUserRequest{Status: "INACTIVE"}
	return c.UpdateUser(id, req)
}

func (c *Client) EnableUser(id string) (*User, error) {
	req := UpdateUserRequest{Status: "ACTIVE"}
	return c.UpdateUser(id, req)
}

// Returns the User with the given email or nil if it does not exist.
func (c *Client) FindUser(email string) (*User, error) {
	users, err := c.ListUsers()
	if err != nil {
		return nil, err
	}
	for _, user := range users {
		if user.Email == email {
			return &user, nil
		}
	}
	return nil, nil
}

func (c *Client) GetUser(id string) (*User, error) {
	var result getUserResponse
	err := c.Get(makePath(PathUsers, id), nil, nil, &result)
	if err != nil {
		return nil, err
	}
	return &result.User, nil
}

func (c *Client) ListUsers() ([]User, error) {
	var result listUsersResponse
	err := c.Get(PathUsers, nil, nil, &result)
	if err != nil {
		return nil, err
	}
	return result.Users, nil
}

func (c *Client) UpdateUser(id string, req UpdateUserRequest) (*User, error) {
	var result updateUserResponse
	err := c.Patch(makePath(PathUsers, id), nil, &req, &result)
	if err != nil {
		return nil, err
	}
	return &result.User, nil
}

//
// Integrations
//

func (c *Client) CreateSnowflakeIntegration(
	name, snowflakeAccount string, adminCreds, proxyCreds *SnowflakeCredentials,
) (*Integration, error) {
	var result Integration
	req := createSnowflakeIntegrationRequest{Name: name}
	req.Snowflake.Account = snowflakeAccount
	req.Snowflake.Admin = *adminCreds
	req.Snowflake.Proxy = *proxyCreds
	if err := c.Post(PathIntegrations, nil, &req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) UpdateSnowflakeIntegration(
	name, raiClientID, raiClientSecret string, proxyCreds *SnowflakeCredentials,
) error {
	var result Integration
	req := updateSnowflakeIntegrationRequest{Name: name}
	req.Snowflake.Proxy = *proxyCreds
	req.RAI.ClientID = raiClientID
	req.RAI.ClientSecret = raiClientSecret
	return c.Patch(PathIntegrations, nil, &req, &result)
}

func (c *Client) DeleteSnowflakeIntegration(name string, adminCreds *SnowflakeCredentials) error {
	req := deleteSnowflakeIntegrationRequest{}
	req.Snowflake.Admin = *adminCreds
	return c.Delete(makePath(PathIntegrations, name), nil, &req, nil)
}

func (c *Client) GetSnowflakeIntegration(name string) (*Integration, error) {
	var result Integration
	if err := c.Get(makePath(PathIntegrations, name), nil, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) ListSnowflakeIntegrations() ([]Integration, error) {
	var result []Integration
	if err := c.Get(PathIntegrations, nil, nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

//
// Snowflake Database Links
//

func (c *Client) CreateSnowflakeDatabaseLink(
	integration, database, schema, role string, creds *SnowflakeCredentials,
) (*SnowflakeDatabaseLink, error) {
	var result SnowflakeDatabaseLink
	path := makePath(PathIntegrations, integration, "database-links")
	req := createSnowflakeDatabaseLinkRequest{}
	req.Snowflake.Database = database
	req.Snowflake.Schema = schema
	req.Snowflake.Role = role
	req.Snowflake.Credentials = *creds
	if err := c.Post(path, nil, &req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) UpdateSnowflakeDatabaseLink(
	integration, database, schema, role string, creds *SnowflakeCredentials,
) error {
	var result SnowflakeDatabaseLink
	name := fmt.Sprintf("%s.%s", database, schema)
	path := makePath(PathIntegrations, integration, "database-links", name)
	req := updateSnowflakeDatabaseLinkRequest{}
	req.Snowflake.Role = role
	req.Snowflake.Credentials = *creds
	return c.Patch(path, nil, &req, &result)
}

func (c *Client) DeleteSnowflakeDatabaseLink(
	integration, database, schema, role string, creds *SnowflakeCredentials,
) error {
	name := fmt.Sprintf("%s.%s", database, schema)
	path := makePath(PathIntegrations, integration, "database-links", name)
	req := deleteSnowflakeDatabaseLinkRequest{}
	req.Snowflake.Role = role
	req.Snowflake.Credentials = *creds
	return c.Delete(path, nil, &req, nil)
}

func (c *Client) GetSnowflakeDatabaseLink(
	integration, database, schema string,
) (*SnowflakeDatabaseLink, error) {
	var result SnowflakeDatabaseLink
	name := fmt.Sprintf("%s.%s", database, schema)
	path := makePath(PathIntegrations, integration, "database-links", name)
	if err := c.Get(path, nil, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) ListSnowflakeDatabaseLinks(
	integration string,
) ([]SnowflakeDatabaseLink, error) {
	var result []SnowflakeDatabaseLink
	path := makePath(PathIntegrations, integration, "database-links")
	if err := c.Get(path, nil, nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

//
// Snowflake Data Streams
//

type DataStreamOpts struct {
	RaiDatabase string
	Relation    string
	ObjectName  string
	Role        string
	Warehouse   string
}

// Creates a data stream to replicate data from a Snowflake table/view to a RAI relation.
func (c *Client) CreateSnowflakeDataStream(
	integration, dbLink string, creds *SnowflakeCredentials, opts *DataStreamOpts,
) (*SnowflakeDataStream, error) {
	var result SnowflakeDataStream
	path := makePath(PathIntegrations, integration, "database-links", dbLink, "data-streams")
	req := createSnowflakeDataStreamRequest{}
	req.Snowflake.Object = opts.ObjectName
	req.Snowflake.Role = opts.Role
	req.Snowflake.Warehouse = opts.Warehouse
	req.Snowflake.Credentials = *creds
	req.RAI.Database = opts.RaiDatabase
	req.RAI.Relation = opts.Relation
	if err := c.Post(path, nil, &req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) DeleteSnowflakeDataStream(
	integration, dbLink, objectName, role string, creds *SnowflakeCredentials,
) error {
	path := makePath(PathIntegrations, integration, "database-links", dbLink, "data-streams", objectName)
	req := deleteSnowflakeDataStreamRequest{}
	req.Snowflake.Role = role
	req.Snowflake.Credentials = *creds
	return c.Delete(path, nil, &req, nil)
}

func (c *Client) GetSnowflakeDataStream(
	integration, dbLink, objectName string,
) (*SnowflakeDataStream, error) {
	var result SnowflakeDataStream
	path := makePath(PathIntegrations, integration, "database-links", dbLink, "data-streams", objectName)
	if err := c.Get(path, nil, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) ListSnowflakeDataStreams(
	integration, dbLink string,
) ([]SnowflakeDataStream, error) {
	var result []SnowflakeDataStream
	path := makePath(PathIntegrations, integration, "database-links", dbLink, "data-streams")
	if err := c.Get(path, nil, nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) GetSnowflakeDataStreamStatus(
	integration, dbLink, objectName string,
) (*SnowflakeDataStreamStatus, error) {
	var result SnowflakeDataStreamStatus
	path := makePath(PathIntegrations, integration, "database-links", dbLink, "data-streams", objectName, "status")
	if err := c.Get(path, nil, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
