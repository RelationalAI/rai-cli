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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/relationalai/rai-sdk-go/rai"
	"github.com/relationalai/raicloud-services/pkg/logger"
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

func (a *Action) getBool(name string) bool {
	result, _ := a.cmd.Flags().GetBool(name)
	return result
}

func (a *Action) getInt(name string) int {
	result, _ := a.cmd.Flags().GetInt(name)
	return result
}

func (a *Action) getRune(name string) rune {
	s, _ := a.cmd.Flags().GetString(name)
	if s == "" {
		return rune(0)
	}
	return []rune(s)[0]
}

func (a *Action) getString(name string) string {
	result, _ := a.cmd.Flags().GetString(name)
	return result
}

func (a *Action) getStringArray(name string) []string {
	result, _ := a.cmd.Flags().GetStringArray(name)
	return result
}

func (a *Action) loadConfig() *rai.Config {
	var cfg rai.Config
	fname := a.getString("config")
	profile := a.getString("profile")
	if err := rai.LoadConfigFile(fname, profile, &cfg); err != nil {
		fmt.Printf("\n%s\n", strings.TrimRight(err.Error(), "\r\n"))
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
	fmt.Printf(format, args...)
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
	fmt.Print(msg)
	return a
}

// Update the action banner and exit.
func (a *Action) Exit(result interface{}, err error) {
	delta := time.Since(a.start).Seconds()
	if err != nil {
		a.Append("(%.1fs)\n%s\n", delta, rtrimEol(err.Error()))
		logger.Info(err.Error())
		os.Exit(1)
	} else {
		a.Append("Ok (%.1fs)\n", delta)
		a.showValue(result)
		os.Exit(0)
	}
}

// Pick a random PROVISIONED engine.
func pickRandomEngine(action *Action) string {
	items, err := action.Client().ListEngines("state", "PROVISIONED")
	if err != nil {
		action.Exit(nil, err)
	}
	switch len(items) {
	case 0:
		action.Exit(nil, ErrNoEngines)
	case 1:
		return items[0].Name
	}
	ix := rand.Intn(len(items))
	return items[ix].Name
}

// Pick the most recently created PROVISIONED engine.
func pickLatestEngine(action *Action) string {
	items, err := action.Client().ListEngines("state", "PROVISIONED")
	if err != nil {
		action.Exit(nil, err)
	}
	var best *rai.Engine
	for i := 0; i < len(items); i++ {
		item := &items[i]
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

// Answers if the given state represents a terminal state.
func isTerminalState(state, targetState string) bool {
	if state == targetState {
		return true
	}
	if strings.Contains(state, "FAILED") {
		return true
	}
	return false
}

func isStatusNotFound(err error) bool {
	e, ok := err.(*rai.HTTPError)
	if !ok {
		return false
	}
	return e.StatusCode == http.StatusNotFound
}

// Wait for the engine to reach the given target state.
func waitEngine(action *Action, name, targetState string) (*rai.Engine, error) {
	for {
		time.Sleep(2 * time.Second)
		item, err := action.Client().GetEngine(name)
		if err != nil {
			if isStatusNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		if item == nil || isTerminalState(item.State, targetState) {
			return item, err
		}
	}
}

//
// Databases
//

func cloneDatabase(cmd *cobra.Command, args []string) {
	// assert len(args) == 2
	action := newAction(cmd)
	name, source := args[0], args[1]
	engine := action.getString("engine")
	overwrite := action.getBool("overwrite")
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("Clone database '%s' from '%s' (/%s)", name, source, engine)
	rsp, err := action.Client().CreateDatabase(name, engine, overwrite)
	action.Exit(rsp, err)
}

func createDatabase(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	action := newAction(cmd)
	name := args[0]
	engine := action.getString("engine")
	overwrite := action.getBool("overwrite")
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("Create database '%s' (/%s)", name, engine)
	rsp, err := action.Client().CreateDatabase(name, engine, overwrite)
	action.Exit(rsp, err)
}

func deleteDatabase(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	action := newAction(cmd)
	name := args[0]
	action.Start("Delete database '%s'", name)
	err := action.Client().DeleteDatabase(name)
	action.Exit(nil, err)
}

func getDatabase(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	action := newAction(cmd)
	name := args[0]
	action.Start("Get database '%s", name)
	item, err := action.Client().GetDatabase(name)
	action.Exit(item, err)
}

func listDatabases(cmd *cobra.Command, args []string) {
	// assert len(args) == 0
	action := newAction(cmd)
	filters := map[string]interface{}{}
	state := action.getStringArray("state")
	if state != nil {
		filters["state"] = state
	}
	action.Start("List databases")
	items, err := action.Client().ListDatabases(filters)
	action.Exit(items, err)
}

//
// Engines
//

func createEngine(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	name := args[0]
	size, _ := cmd.Flags().GetString("size")
	nowait, _ := cmd.Flags().GetBool("nowait")
	action := newAction(cmd).Start("Create engine '%s' size=%s", name, size)
	item, err := action.Client().CreateEngine(name, size)
	if err == nil && !nowait {
		item, err = waitEngine(action, name, "PROVISIONED")
	}
	action.Exit(item, err)
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
	action := newAction(cmd)
	engine := args[0]
	action.Start("Get engine '%s", engine)
	item, err := action.Client().GetEngine(engine)
	action.Exit(item, err)
}

func listEngines(cmd *cobra.Command, args []string) {
	// assert len(args) == 0
	action := newAction(cmd)
	filters := map[string][]string{}
	state := action.getStringArray("state")
	if state != nil {
		filters["state"] = state
	}
	action.Start("List engines")
	items, err := action.Client().ListEngines(filters)
	action.Exit(items, err)
}

//
// OAuth Clients
//

func createOAuthClient(cmd *cobra.Command, args []string) {
	// assert len(args) == 0
	action := newAction(cmd)
	name := args[0]
	perms := action.getStringArray("perms")
	action.Start("Create OAuth Client '%s' perms=%s", name, strings.Join(perms, ","))
	item, err := action.Client().CreateOAuthClient(name, perms)
	action.Exit(item, err)
}

func deleteOAuthClient(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	id := args[0]
	action := newAction(cmd).Start("Delete OAuth Client '%s'", id)
	item, err := action.Client().DeleteOAuthClient(id)
	action.Exit(item, err)
}

func getOAuthClient(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	id := args[0]
	action := newAction(cmd).Start("Get OAuth Client '%s'", id)
	item, err := action.Client().GetOAuthClient(id)
	action.Exit(item, err)
}

func listOAuthClients(cmd *cobra.Command, args []string) {
	// assert len(args) == 0
	action := newAction(cmd).Start("List OAuth Clients ..")
	items, err := action.Client().ListOAuthClients()
	action.Exit(items, err)
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
	item, err := action.Client().DeleteModels(database, engine, models)
	action.Exit(item, err)
}

func getModel(cmd *cobra.Command, args []string) {
	// assert len(args) == 2
	database := args[0]
	model := args[1]
	engine, _ := cmd.Flags().GetString("engine")
	action := newAction(cmd)
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("Get model '%s' (%s/%s)", model, database, engine)
	item, err := action.Client().GetModel(database, engine, model)
	action.Exit(item, err)
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

func listModels(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	database := args[0]
	engine, _ := cmd.Flags().GetString("engine")
	action := newAction(cmd)
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("List models '%s' (/%s)", database, engine)
	items, err := action.Client().ListModels(database, engine)
	action.Exit(items, err)
}

func loadModels(cmd *cobra.Command, args []string) {
	// assert len(args) >= 2
	database := args[0]
	engine, _ := cmd.Flags().GetString("engine")
	models := map[string]io.Reader{}
	for _, arg := range args[1:] {
		name := baseSansExt(arg)
		r, err := os.Open(arg)
		if err != nil {
			fatal(err.Error())
		}
		models[name] = r
	}
	action := newAction(cmd)
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
func getQuerySource(cmd *cobra.Command, args []string) string {
	source, _ := cmd.Flags().GetString("code")
	if source != "" {
		return source
	}
	fname, _ := cmd.Flags().GetString("file")
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
	database := args[0]
	engine, _ := cmd.Flags().GetString("engine")
	readonly, _ := cmd.Flags().GetBool("readonly")
	source := getQuerySource(cmd, args)
	action := newAction(cmd)
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("Executing query (%s/%s)", database, engine)
	items, err := action.Client().Execute(database, engine, source, nil, readonly)
	action.Exit(items, err)
}

func listEdbs(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	database := args[0]
	engine, _ := cmd.Flags().GetString("engine")
	action := newAction(cmd)
	if engine == "" {
		engine = pickEngine(action)
	}
	action.Start("List EDBs '%s' (/%s)", database, engine)
	items, err := action.Client().ListEDBs(database, engine)
	action.Exit(items, err)
}

// Returns load-csv options specified on command
func getCSVOptions(a *Action) *rai.CSVOptions {
	opts := &rai.CSVOptions{}
	n := a.getInt("header-row")
	if n >= 0 {
		opts.HeaderRow = n
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
	return opts
}

func loadCSV(cmd *cobra.Command, args []string) {
	// assert len(args) == 2
	action := newAction(cmd)
	database := args[0]
	fname := args[1]
	engine, _ := cmd.Flags().GetString("engine")
	relation, _ := cmd.Flags().GetString("relation")
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
	_, err = action.Client().LoadCSV(database, engine, relation, r, opts)
	action.Exit(nil, err) // ignore response
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
	_, err = action.Client().LoadJSON(database, engine, relation, data)
	action.Exit(nil, err) // ignore response
}

//
// Users
//

func createUser(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	action := newAction(cmd)
	email := args[0]
	roles, _ := cmd.Flags().GetStringArray("roles")
	action.Start("Create user '%s' roles=%s", email, strings.Join(roles, ","))
	item, err := action.Client().CreateUser(email, roles)
	action.Exit(item, err)
}

func disableUser(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	id := args[0]
	action := newAction(cmd).Start("Disable user '%s'", id)
	item, err := action.Client().DisableUser(id)
	action.Exit(item, err)
}

func enableUser(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	id := args[0]
	action := newAction(cmd).Start("Enable user '%s'", id)
	item, err := action.Client().EnableUser(id)
	action.Exit(item, err)
}

func getUser(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	id := args[0]
	action := newAction(cmd).Start("Get user '%s'", id)
	item, err := action.Client().GetUser(id)
	action.Exit(item, err)
}

// Returns the user-id corresponding to the given email.
func findUser(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	email := args[0]
	action := newAction(cmd).Start("Find user '%s'", email)
	user, err := action.Client().FindUser(email)
	action.Exit(user, err)
}

func listUsers(cmd *cobra.Command, args []string) {
	// assert len(args) == 0
	action := newAction(cmd).Start("List users")
	items, err := action.Client().ListUsers()
	action.Exit(items, err)
}

func updateUser(cmd *cobra.Command, args []string) {
	// assert len(args) == 1
	action := newAction(cmd)
	id := args[0]
	status := action.getString("status")
	roles := action.getStringArray("roles")
	req := rai.UpdateUserRequest{Status: status, Roles: roles}
	action.Start("Update user '%s' status=%s", id, status)
	item, err := action.Client().UpdateUser(id, req)
	action.Exit(item, err)
}
