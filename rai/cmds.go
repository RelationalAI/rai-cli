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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/relationalai/rai-sdk-go/rai"
	"github.com/spf13/cobra"
)

var ErrNoEngines = errors.New("no engines available")

func fatal(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "Error: %s\n", msg)
	os.Exit(1)
}

func baseSansExt(fname string) string {
	base := filepath.Base(fname)
	return strings.TrimSuffix(base, path.Ext(base))
}

func readFile(fname string) (string, error) {
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Represents the state used when processing a command.
type Action struct {
	cmd    *cobra.Command
	quiet  bool
	client *rai.Client
	start  time.Time
}

func newAction(cmd *cobra.Command) *Action {
	result := &Action{cmd: cmd, start: time.Now()}
	result.quiet = result.getBool("quiet")
	return result
}

func (a *Action) Client() *rai.Client {
	if a.client == nil {
		a.client = a.newClient()
	}
	return a.client
}

func (a *Action) Context() context.Context {
	return a.cmd.Context()
}

// Returns the bool value corresponding to the named flag.
func (a *Action) getBool(name string) bool {
	result, _ := a.cmd.Flags().GetBool(name)
	return result
}

// Returns the int value corresponding to the named flag.
func (a *Action) getInt(name string) int {
	result, _ := a.cmd.Flags().GetInt(name)
	return result
}

// Returns the rune value corresponding to the named flag.
func (a *Action) getRune(name string) rune {
	s, _ := a.cmd.Flags().GetString(name)
	if s == "" {
		return rune(0)
	}
	return []rune(s)[0]
}

// Returns the string value corresponding to the named flag.
func (a *Action) getString(name string) string {
	result, _ := a.cmd.Flags().GetString(name)
	return result
}

// Returns the string value corresponding to the named flag, and if no value
// is set, return the value corresponding to the given environment variable.
func (a *Action) getStringEnv(name, key string) string {
	result, _ := a.cmd.Flags().GetString(name)
	if result == "" {
		result = os.Getenv(key)
	}
	return result
}

// Returns the string array value corresponding to the named flag.
func (a *Action) getStringArray(name string) []string {
	result, _ := a.cmd.Flags().GetStringArray(name)
	return result
}

func (a *Action) loadConfig() *rai.Config {
	var cfg rai.Config
	fname := a.getString("config")
	profile := a.getString("profile")
	if err := rai.LoadConfigFile(fname, profile, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "\n")
		fatal(strings.TrimRight(err.Error(), "\r\n"))
	}
	host := a.getString("host")
	if host != "" {
		cfg.Host = host
	}
	port := a.getString("port")
	if port != "" {
		cfg.Port = port
	}
	return &cfg
}

func (a *Action) newClient() *rai.Client {
	cfg := a.loadConfig()
	opts := &rai.ClientOptions{Config: *cfg}
	return rai.NewClient(a.Context(), opts)
}

func isNil(v interface{}) bool {
	switch v.(type) {
	case string:
		return false
	}
	return v == nil || reflect.ValueOf(v).IsNil()
}

func rtrimEol(value string) string {
	return strings.TrimRight(value, "\r\n")
}

func showJSON(v interface{}) {
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	e.Encode(v)
}

func (a *Action) showValue(v interface{}) {
	switch vv := v.(type) {
	case string:
		fmt.Println(rtrimEol(vv))
	default:
		if isNil(v) {
			return
		}
		format := a.getString("format")
		switch format {
		case "pretty":
			if s, ok := v.(rai.Showable); ok {
				s.Show()
				return
			}
		case "json":
			break // default
		}
		showJSON(v)
	}
}

func (a *Action) Append(format string, args ...interface{}) *Action {
	if a.quiet {
		return a
	}
	fmt.Fprintf(os.Stderr, format, args...)
	return a
}

// Show the action banner message.
func (a *Action) Start(format string, args ...interface{}) *Action {
	if a.quiet {
		return a
	}
	var msg string
	msg = fmt.Sprintf(format, args...)
	msg = fmt.Sprintf("%s .. ", msg)
	fmt.Fprintln(os.Stderr, msg)
	return a
}

// Update the action banner and exit.
func (a *Action) Exit(result interface{}, err error) {
	delta := time.Since(a.start).Seconds()
	if err != nil {
		a.Append("(%.1fs)\n%s\n", delta, rtrimEol(err.Error()))
		os.Exit(1)
	} else {
		a.Append("Ok (%.1fs)\n", delta)
		a.showValue(result)
		os.Exit(0)
	}
}

// Pick a random PROVISIONED engine.
/*
func pickRandomEngine(action *Action) string {
	rsp, err := action.Client().ListEngines("state", "PROVISIONED")
	if err != nil {
		action.Exit(nil, err)
	}
	switch len(rsp) {
	case 0:
		action.Exit(nil, ErrNoEngines)
	case 1:
		return rsp[0].Name
	}
	ix := rand.Intn(len(rsp))
	return rsp[ix].Name
}
*/

// Pick the most recently created PROVISIONED engine.
func pickLatestEngine(action *Action) string {
	rsp, err := action.Client().ListEngines("state", "PROVISIONED")
	if err != nil {
		action.Exit(nil, err)
	}
	var best *rai.Engine
	for i := 0; i < len(rsp); i++ {
		item := &rsp[i]
		if best == nil || best.CreatedOn < item.CreatedOn {
			best = item
		}
	}
	if best == nil {
		action.Exit(nil, ErrNoEngines)
	}
	return best.Name
}

// todo: switch to `pickRandomEngine` when we have database compatibility,
// currently its too likely to pick an incompatible engine.
func pickEngine(action *Action) string {
	return pickLatestEngine(action)
}

//
// Databases
//

func cloneDatabase(cmd *cobra.Command, args []string) {
	// assert len(args) == 2
	name, source := args[0], args[1]
	action := newAction(cmd)
	action.Start("Clone database '%s' from '%s'", name, source)
	rsp, err := action.Client().CloneDatabase(name, source)
	action.Exit(rsp, err)
}

func createDatabase(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	name := args[0]
	action := newAction(cmd)
	action.Start("Create database '%s'", name)
	rsp, err := action.Client().CreateDatabase(name)
	action.Exit(rsp, err)
}

func deleteDatabase(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	name := args[0]
	action := newAction(cmd)
	action.Start("Delete database '%s'", name)
	err := action.Client().DeleteDatabase(name)
	action.Exit(nil, err)
}

func getDatabase(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	name := args[0]
	action := newAction(cmd)
	action.Start("Get database '%s'", name)
	rsp, err := action.Client().GetDatabase(name)
	action.Exit(rsp, err)
}

func listDatabases(cmd *cobra.Command, args []string) {
	// assert len(args) == 0
	filters := map[string]interface{}{}
	action := newAction(cmd)
	state := action.getStringArray("state")
	if state != nil {
		filters["state"] = state
	}
	action.Start("List databases")
	rsp, err := action.Client().ListDatabases(filters)
	action.Exit(rsp, err)
}

//
// Engines
//

func createEngine(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	name := args[0]
	action := newAction(cmd)
	size := action.getString("size")
	action.Start("Create engine '%s' size=%s", name, size)
	rsp, err := action.Client().CreateEngine(name, size)
	action.Exit(rsp, err)
}

func deleteEngine(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	action := newAction(cmd)
	name := args[0]
	action.Start("Delete engine '%s'", name)
	err := action.Client().DeleteEngine(name)
	action.Exit(nil, err)
}

func getEngine(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	engine := args[0]
	action := newAction(cmd)
	action.Start("Get engine '%s'", engine)
	rsp, err := action.Client().GetEngine(engine)
	action.Exit(rsp, err)
}

func listEngines(cmd *cobra.Command, args []string) {
	// assert len(args) == 0
	filters := map[string]interface{}{}
	action := newAction(cmd)
	state := action.getStringArray("state")
	if state != nil {
		filters["state"] = state
	}
	action.Start("List engines")
	rsp, err := action.Client().ListEngines(filters)
	action.Exit(rsp, err)
}

//
// OAuth Clients
//

func createOAuthClient(cmd *cobra.Command, args []string) {
	// assert len(args) == 0
	name := args[0]
	action := newAction(cmd)
	perms := action.getStringArray("perms")
	action.Start("Create OAuth Client '%s' perms=%s", name, strings.Join(perms, ","))
	rsp, err := action.Client().CreateOAuthClient(name, perms)
	action.Exit(rsp, err)
}

func deleteOAuthClient(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	id := args[0]
	action := newAction(cmd).Start("Delete OAuth Client '%s'", id)
	rsp, err := action.Client().DeleteOAuthClient(id)
	action.Exit(rsp, err)
}

func findOAuthClient(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	name := args[0]
	action := newAction(cmd).Start("Find OAuth Client '%s'", name)
	rsp, err := action.Client().FindOAuthClient(name)
	action.Exit(rsp, err)
}

func getOAuthClient(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	id := args[0]
	action := newAction(cmd).Start("Get OAuth Client '%s'", id)
	rsp, err := action.Client().GetOAuthClient(id)
	action.Exit(rsp, err)
}

func listOAuthClients(cmd *cobra.Command, args []string) {
	// assert len(args) == 0
	action := newAction(cmd).Start("List OAuth Clients ..")
	rsp, err := action.Client().ListOAuthClients()
	action.Exit(rsp, err)
}

//
// Models
//

func deleteModel(cmd *cobra.Command, args []string) {
	// assert len(args) >= 2
	action := newAction(cmd)
	database := args[0]
	models := args[1:]
	engine := action.getString("engine")
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("Delete model '%s' (%s/%s)", strings.Join(models, ", "), database, engine)
	rsp, err := action.Client().DeleteModels(database, engine, models)
	action.Exit(rsp, err)
}

func getModel(cmd *cobra.Command, args []string) {
	// assert len(args) == 2
	database := args[0]
	model := args[1]
	action := newAction(cmd)
	engine := action.getString("engine")
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("Get model '%s' (%s/%s)", model, database, engine)
	rsp, err := action.Client().GetModel(database, engine, model)
	action.Exit(rsp, err)
}

func getModelSource(cmd *cobra.Command, args []string) {
	// assert len(args) == 2
	database := args[0]
	model := args[1]
	action := newAction(cmd)
	engine := action.getString("engine")
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("Get model source '%s' (%s/%s)", model, database, engine)
	rsp, err := action.Client().GetModel(database, engine, model)
	if err != nil {
		action.Exit(nil, err)
	}
	action.Exit(rsp.Value, nil)
}

// Return the list of keys corresponding to the given map.
func mapKeys(m map[string]io.Reader) []string {
	i := 0
	keys := make([]string, len(m))
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}

func listModelNames(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	database := args[0]
	action := newAction(cmd)
	engine := action.getString("engine")
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("List model names '%s' (/%s)", database, engine)
	rsp, err := action.Client().ListModelNames(database, engine)
	action.Exit(rsp, err)
}

func listModels(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	database := args[0]
	action := newAction(cmd)
	engine := action.getString("engine")
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("List models '%s' (/%s)", database, engine)
	rsp, err := action.Client().ListModels(database, engine)
	action.Exit(rsp, err)
}

// Load a single model, with the option of setting the model name.
func loadModel(cmd *cobra.Command, args []string) {
	// assert len(args) == 2
	database, fname := args[0], args[1]
	r, err := os.Open(fname)
	if err != nil {
		fatal(err.Error())
	}
	action := newAction(cmd)
	mname := action.getString("model")
	if mname == "" {
		mname = baseSansExt(fname)
	}
	engine := action.getString("engine")
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("Load model '%s' as '%s' (%s/%s)", fname, mname, database, engine)
	_, err = action.Client().LoadModel(database, engine, mname, r)
	action.Exit(nil, err) // ignore response
}

// Load one or more models, using the file names for the model name.
func loadModels(cmd *cobra.Command, args []string) {
	// assert len(args) >= 2
	database := args[0]
	action := newAction(cmd)
	engine := action.getString("engine")
	prefix := action.getString("prefix")
	models := map[string]io.Reader{}
	for _, arg := range args[1:] {
		name := filepath.Join(prefix, baseSansExt(arg))
		r, err := os.Open(arg)
		if err != nil {
			fatal(err.Error())
		}
		models[name] = r
	}
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("Load models '%s' (%s/%s)", strings.Join(mapKeys(models), ", "), database, engine)
	_, err := action.Client().LoadModels(database, engine, models)
	action.Exit(nil, err) // ignore response
}

//
// Transactions
//

// Retrieve query source from command option or named source file.
func getQuerySource(action *Action, args []string) string {
	source := action.getString("code")
	if source != "" {
		return source
	}
	fname := action.getString("file")
	if fname == "" {
		fatal("nothing to execute")
	}
	var err error
	if source, err = readFile(fname); err != nil {
		fatal(err.Error())
	}
	return source
}

func execQuery(cmd *cobra.Command, args []string) {
	action := newAction(cmd)
	database := args[0]
	source := getQuerySource(action, args)
	readonly := action.getBool("readonly")
	engine := action.getString("engine")
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("Executing query (%s/%s) readonly=%s", database, engine, strconv.FormatBool(readonly))
	rsp, err := action.Client().Execute(database, engine, source, nil, readonly)
	action.Exit(rsp, err)
}

func listEdbs(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	action := newAction(cmd)
	database := args[0]
	engine := action.getString("engine")
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("List EDBs '%s' (/%s)", database, engine)
	rsp, err := action.Client().ListEDBs(database, engine)
	action.Exit(rsp, err)
}

func listEdbNames(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	action := newAction(cmd)
	database := args[0]
	engine := action.getString("engine")
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("List EDB names '%s' (/%s)", database, engine)
	rsp, err := action.Client().ListEDBs(database, engine)
	if err != nil {
		action.Exit(nil, err)
	}
	nameMap := map[string]bool{}
	for i := 0; i < len(rsp); i++ {
		edb := &rsp[i]
		nameMap[edb.Name] = true
	}
	names := make([]string, 0, len(nameMap))
	for name := range nameMap {
		names = append(names, name)
	}
	sort.Strings(names)
	action.Exit(names, nil)
}

// Parse the schema option string into the schema definition map that is
// expected by the golang client.
//
// The schema definition consists of a sequence of semicolon delimited
// <column>:<type> pairs that are parsed into a column => type map, eg:
//
//	--schema='cocktail:string;quantity:int;price:decimal(64,2);date:date'
func parseSchema(a *Action) map[string]string {
	schema := a.getString("schema")
	if schema == "" {
		return nil
	}
	result := map[string]string{}
	parts := strings.Split(schema, ";")
	for _, part := range parts {
		item := strings.Split(part, ":")
		if len(item) != 2 {
			fatal("bad schema definition '%s', expected '<column>:<type>'", item)
		}
		result[item[0]] = item[1]
	}
	return result
}

// Returns load-csv options specified on command
func getCSVOptions(a *Action) *rai.CSVOptions {
	opts := &rai.CSVOptions{}
	n := a.getInt("header-row")
	if n >= 0 {
		opts.HeaderRow = &n
	}
	c := a.getRune("delim")
	if c != 0 {
		opts.Delim = c
	}
	c = a.getRune("escapechar")
	if c != 0 {
		opts.EscapeChar = c
	}
	c = a.getRune("quotechar")
	if c != 0 {
		opts.QuoteChar = c
	}
	opts.Schema = parseSchema(a)
	return opts
}

func loadCSV(cmd *cobra.Command, args []string) {
	// assert len(args) == 2
	action := newAction(cmd)
	database, fname := args[0], args[1]
	engine := action.getString("engine")
	relation := action.getString("relation")
	if relation == "" {
		relation = baseSansExt(fname)
	}
	r, err := os.Open(fname)
	if err != nil {
		fatal(err.Error())
	}
	opts := getCSVOptions(action)
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("Load CSV '%s' (%s/%s)", relation, database, engine)
	rsp, err := action.Client().LoadCSV(database, engine, relation, r, opts)
	action.Exit(rsp, err) // ignore response
}

func loadJSON(cmd *cobra.Command, args []string) {
	// assert len(args) == 2
	action := newAction(cmd)
	database := args[0]
	fname := args[1]
	relation := action.getString("relation")
	if relation == "" {
		relation = baseSansExt(fname)
	}
	data, err := os.Open(fname)
	if err != nil {
		fatal(err.Error())
	}
	engine := action.getString("engine")
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("Load JSON '%s' (%s/%s)", relation, database, engine)
	rsp, err := action.Client().LoadJSON(database, engine, relation, data)
	action.Exit(rsp, err) // ignore response
}

//
// Users
//

func createUser(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	action := newAction(cmd)
	email := args[0]
	roles := action.getStringArray("role")
	action.Start("Create user '%s' roles=%s", email, strings.Join(roles, ","))
	rsp, err := action.Client().CreateUser(email, roles)
	action.Exit(rsp, err)
}

func deleteUser(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	id := args[0]
	action := newAction(cmd).Start("Delete user '%s'", id)
	rsp, err := action.Client().DeleteUser(id)
	action.Exit(rsp, err)
}

func disableUser(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	id := args[0]
	action := newAction(cmd).Start("Disable user '%s'", id)
	rsp, err := action.Client().DisableUser(id)
	action.Exit(rsp, err)
}

func enableUser(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	id := args[0]
	action := newAction(cmd).Start("Enable user '%s'", id)
	rsp, err := action.Client().EnableUser(id)
	action.Exit(rsp, err)
}

func getUser(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	id := args[0]
	action := newAction(cmd).Start("Get user '%s'", id)
	rsp, err := action.Client().GetUser(id)
	action.Exit(rsp, err)
}

// Returns the user-id corresponding to the given email.
func findUser(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	email := args[0]
	action := newAction(cmd).Start("Find user '%s'", email)
	rsp, err := action.Client().FindUser(email)
	action.Exit(rsp, err)
}

func listUsers(cmd *cobra.Command, args []string) {
	// assert len(args) == 0
	action := newAction(cmd).Start("List users")
	rsp, err := action.Client().ListUsers()
	action.Exit(rsp, err)
}

func updateUser(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	action := newAction(cmd)
	id := args[0]
	status := action.getString("status")
	roles := action.getStringArray("roles")
	req := rai.UpdateUserRequest{Status: status, Roles: roles}
	action.Start("Update user '%s' status=%s", id, status)
	rsp, err := action.Client().UpdateUser(id, req)
	action.Exit(rsp, err)
}

//
// Snowflake integrations
//

func createSnowflakeIntegration(cmd *cobra.Command, args []string) {
	action := newAction(cmd)
	name := args[0]
	account := action.getStringEnv("account", "SNOWSQL_ACCOUNT")
	adminUsername := action.getStringEnv("admin-username", "SNOWSQL_USER")
	adminPassword := action.getStringEnv("admin-password", "SNOWSQL_PWD")
	proxyUsername := action.getStringEnv("proxy-username", "SNOWSQL_USER")
	proxyPassword := action.getStringEnv("proxy-password", "SNOWSQL_PWD")
	adminCreds := rai.SnowflakeCredentials{
		Username: adminUsername, Password: adminPassword}
	proxyCreds := rai.SnowflakeCredentials{
		Username: proxyUsername, Password: proxyPassword}
	action.Start("Create Snowflake integration '%s' account='%s'", name, account)
	rsp, err := action.Client().CreateSnowflakeIntegration(
		name, account, &adminCreds, &proxyCreds)
	action.Exit(rsp, err)
}

func deleteSnowflakeIntegration(cmd *cobra.Command, args []string) {
	action := newAction(cmd)
	name := args[0]
	username := action.getStringEnv("username", "SNOWSQL_USER")
	password := action.getStringEnv("password", "SNOWSQL_PWD")
	creds := rai.SnowflakeCredentials{Username: username, Password: password}
	action.Start("Delete Snowflake integration '%s'", name)
	err := action.Client().DeleteSnowflakeIntegration(name, &creds)
	action.Exit(nil, err)
}

func getSnowflakeIntegration(cmd *cobra.Command, args []string) {
	action := newAction(cmd)
	name := args[0]
	action.Start("Get Snowflake integration '%s'", name)
	rsp, err := action.Client().GetSnowflakeIntegration(name)
	action.Exit(rsp, err)
}

func listSnowflakeIntegrations(cmd *cobra.Command, _ []string) {
	action := newAction(cmd)
	action.Start("List Snowflake integrations")
	rsp, err := action.Client().ListSnowflakeIntegrations()
	action.Exit(rsp, err)
}

//
// Snowflake database links
//

func createSnowflakeDatabaseLink(cmd *cobra.Command, args []string) {
	action := newAction(cmd)
	integration := args[0]
	database := action.getStringEnv("database", "SNOWSQL_DATABASE")
	schema := action.getStringEnv("schema", "SNOWSQL_SCHEMA")
	role := action.getStringEnv("role", "SNOWSQL_ROLE")
	username := action.getStringEnv("username", "SNOWSQL_USER")
	password := action.getStringEnv("password", "SNOWSQL_PWD")
	creds := rai.SnowflakeCredentials{Username: username, Password: password}
	name := fmt.Sprintf("%s.%s", database, schema)
	action.Start("Create Snowflake database link '%s' (%s)", name, integration)
	rsp, err := action.Client().CreateSnowflakeDatabaseLink(
		integration, database, schema, role, &creds)
	action.Exit(rsp, err)
}

func deleteSnowflakeDatabaseLink(cmd *cobra.Command, args []string) {
	action := newAction(cmd)
	integration := args[0]
	database := action.getStringEnv("database", "SNOWSQL_DATABASE")
	schema := action.getStringEnv("schema", "SNOWSQL_SCHEMA")
	role := action.getStringEnv("role", "SNOWSQL_ROLE")
	username := action.getStringEnv("username", "SNOWSQL_USER")
	password := action.getStringEnv("password", "SNOWSQL_PWD")
	creds := rai.SnowflakeCredentials{Username: username, Password: password}
	name := fmt.Sprintf("%s.%s", database, schema)
	action.Start("Delete Snowflake database link '%s' (%s)", name, integration)
	err := action.Client().DeleteSnowflakeDatabaseLink(
		integration, database, schema, role, &creds)
	action.Exit(nil, err)
}

func getSnowflakeDatabaseLink(cmd *cobra.Command, args []string) {
	action := newAction(cmd)
	integration := args[0]
	database := action.getStringEnv("database", "SNOWSQL_DATABASE")
	schema := action.getStringEnv("schema", "SNOWSQL_SCHEMA")
	name := fmt.Sprintf("%s.%s", database, schema)
	action.Start("Get Snowflake database link '%s' (%s)", name, integration)
	rsp, err := action.Client().GetSnowflakeDatabaseLink(integration, database, schema)
	action.Exit(rsp, err)
}

func listSnowflakeDatabaseLinks(cmd *cobra.Command, args []string) {
	action := newAction(cmd)
	integration := args[0]
	action.Start("List Snowflake database links (%s)", integration)
	rsp, err := action.Client().ListSnowflakeDatabaseLinks(integration)
	action.Exit(rsp, err)
}

//
// Snowflake database links
//

func createSnowflakeDatastream(cmd *cobra.Command, args []string) {
	action := newAction(cmd)
	integration := args[0]
	dataStream := args[1]
	role := action.getStringEnv("role", "SNOWSQL_ROLE")
	warehouse := action.getStringEnv("warehouse", "SNOWSQL_WAREHOUSE")
	username := action.getStringEnv("username", "SNOWSQL_USER")
	password := action.getStringEnv("password", "SNOWSQL_PWD")
	dbLink := action.getString("database-link")
	isView := action.getBool("is-view")
	raiDatabase := action.getString("rai-database")
	relation := action.getString("relation")
	creds := &rai.SnowflakeCredentials{Username: username, Password: password}

	action.Start("Create Snowflake data stream '%s' (%s)", dataStream, integration)
	rsp, err := action.Client().CreateSnowflakeDataStream(
		integration, dbLink, dataStream,
		role, warehouse, isView, raiDatabase, relation, creds,
	)
	action.Exit(rsp, err)
}
func deleteSnowflakeDatastream(cmd *cobra.Command, args []string) {
	action := newAction(cmd)
	integration := args[0]
	dataStream := args[1]
	role := action.getStringEnv("role", "SNOWSQL_ROLE")
	username := action.getStringEnv("username", "SNOWSQL_USER")
	password := action.getStringEnv("password", "SNOWSQL_PWD")
	dbLink := action.getString("database-link")
	creds := rai.SnowflakeCredentials{Username: username, Password: password}
	action.Start("Delete Snowflake data stream %s (%s)", dataStream, integration)
	err := action.Client().DeleteSnowflakeDataStream(
		integration, dbLink, dataStream, role, &creds,
	)
	action.Exit(nil, err)
}
func getSnowflakeDatastream(cmd *cobra.Command, args []string) {
	action := newAction(cmd)
	integration := args[0]
	dataStream := args[1]
	dbLink := action.getString("database-link")
	action.Start("Get Snowflake data stream %s (%s)", dataStream, integration)
	rsp, err := action.Client().GetSnowflakeDataStream(integration, dbLink, dataStream)
	action.Exit(rsp, err)
}

func listSnowflakeDatastreams(cmd *cobra.Command, args []string) {
	action := newAction(cmd)
	integration := args[0]
	dbLink := action.getString("database-link")
	action.Start("List Snowflake datastreams linked to %s (%s)", dbLink, integration)
	rsp, err := action.Client().ListSnowflakeDataStreams(integration, dbLink)
	action.Exit(rsp, err)
}

//
// Misc
//

func getAccessToken(cmd *cobra.Command, args []string) {
	// assert len(args) == 0
	action := newAction(cmd)
	action.Start("Get access token")
	rsp, err := action.Client().AccessToken()
	action.Exit(rsp, err)
}
