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
	"github.com/spf13/cobra"
)

func addCommands(root *cobra.Command) {
	// Databses
	cmd := &cobra.Command{
		Use:   "clone-database database source-database",
		Short: "Clone a database",
		Args:  cobra.ExactArgs(2),
		Run:   cloneDatabase}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "create-database database",
		Short: "Create a database",
		Args:  cobra.ExactArgs(1),
		Run:   createDatabase}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "delete-database database",
		Short: "Delete a database",
		Args:  cobra.ExactArgs(1),
		Run:   deleteDatabase}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "get-database database",
		Short: "Get information about the given database",
		Args:  cobra.ExactArgs(1),
		Run:   getDatabase}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "list-databases",
		Short: "List all databases",
		Run:   listDatabases}
	cmd.Flags().StringArray("state", nil, "database state filter")
	root.AddCommand(cmd)

	// Engines
	cmd = &cobra.Command{
		Use:   "create-engine engine",
		Short: "Create an engine",
		Args:  cobra.ExactArgs(1),
		Run:   createEngine}
	cmd.Flags().String("size", "XS", "engine size (default: XS)")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "delete-engine engine",
		Short: "Delete an engine",
		Args:  cobra.ExactArgs(1),
		Run:   deleteEngine}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "get-engine engine",
		Short: "Get information about the given engine",
		Args:  cobra.ExactArgs(1),
		Run:   getEngine}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "list-engines",
		Short: "List all engines",
		Run:   listEngines}
	cmd.Flags().StringArray("state", nil, "engine state filter")
	root.AddCommand(cmd)

	// Models
	cmd = &cobra.Command{
		Use:   "delete-models database model+",
		Short: "Delete models in the given database",
		Args:  cobra.MinimumNArgs(2),
		Run:   deleteModel}
	cmd.Flags().StringP("engine", "e", "", "default engine")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "get-model database model",
		Short: "Get details for the given model",
		Args:  cobra.ExactArgs(2),
		Run:   getModel}
	cmd.Flags().StringP("engine", "e", "", "default engine")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "get-model-source database model",
		Short: "Get the source text for the given model",
		Args:  cobra.ExactArgs(2),
		Run:   getModelSource}
	cmd.Flags().StringP("engine", "e", "", "default engine")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "load-model database file",
		Short: "Load model into the given database",
		Args:  cobra.ExactArgs(2),
		Run:   loadModel}
	cmd.Flags().StringP("engine", "e", "", "default engine")
	cmd.Flags().StringP("model", "m", "", "model name (default: file name)")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "load-models database file+",
		Short: "Load models into the given database",
		Args:  cobra.MinimumNArgs(2),
		Run:   loadModels}
	cmd.Flags().StringP("engine", "e", "", "default engine")
	cmd.Flags().StringP("prefix", "p", "", "namespace prefix")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "list-models database",
		Short: "List all models in the given database",
		Args:  cobra.ExactArgs(1),
		Run:   listModels}
	cmd.Flags().StringP("engine", "e", "", "default engine")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "list-model-names database",
		Short: "List the names of all models in the given database",
		Args:  cobra.ExactArgs(1),
		Run:   listModelNames}
	cmd.Flags().StringP("engine", "e", "", "default engine")
	root.AddCommand(cmd)

	// OAuth Clients
	cmd = &cobra.Command{
		Use:   "create-oauth-client name",
		Short: "Create an OAuth client",
		Args:  cobra.ExactArgs(1),
		Run:   createOAuthClient}
	cmd.Flags().StringArray("perms", nil, "permissions")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "delete-oauth-client client-id",
		Short: "Delete an OAuth client",
		Args:  cobra.ExactArgs(1),
		Run:   deleteOAuthClient}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "find-oauth-client client-name",
		Short: "Get information about the OAuth client with the given client-name",
		Args:  cobra.ExactArgs(1),
		Run:   findOAuthClient}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "get-oauth-client client-id",
		Short: "Get information about the OAuth client with the given client-id",
		Args:  cobra.ExactArgs(1),
		Run:   getOAuthClient}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "list-oauth-clients",
		Short: "List all OAuth clients",
		Run:   listOAuthClients}
	root.AddCommand(cmd)

	// Transactions
	cmd = &cobra.Command{
		Use:   "exec database",
		Short: "Execute a transaction on the given database",
		Args:  cobra.ExactArgs(1),
		Run:   execQuery}
	cmd.Flags().StringP("engine", "e", "", "default engine")
	cmd.Flags().StringP("code", "c", "", "rel source code")
	cmd.Flags().StringP("file", "f", "", "rel source file")
	cmd.Flags().Bool("readonly", false, "transaction is read-only")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "list-edbs database",
		Short: "List all EDBs in the given database",
		Args:  cobra.ExactArgs(1),
		Run:   listEdbs}
	cmd.Flags().StringP("engine", "e", "", "default engine")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "list-edb-names database",
		Short: "List the names of all EDBs in the given database",
		Args:  cobra.ExactArgs(1),
		Run:   listEdbNames}
	cmd.Flags().StringP("engine", "e", "", "default engine")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "load-csv database file",
		Short: "Load a CSV file into the given database",
		Args:  cobra.ExactArgs(2),
		Run:   loadCSV}
	cmd.Flags().StringP("engine", "e", "", "default engine")
	cmd.Flags().Int("header-row", -1, "header row number, 0 for no header (default: 1)")
	cmd.Flags().String("delim", "", "field delimiter")
	cmd.Flags().String("escapechar", "", "character used to escape quotes")
	cmd.Flags().String("quotechar", "", "quoted field character")
	cmd.Flags().String("schema", "", "schema definition")
	cmd.Flags().StringP("relation", "r", "", "relation name (default: file name)")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "load-json database file",
		Short: "Load a JSON file into the given database",
		Args:  cobra.MinimumNArgs(1),
		Run:   loadJSON}
	cmd.Flags().StringP("engine", "e", "", "default engine")
	cmd.Flags().StringP("relation", "r", "", "relation name (default: file name)")
	root.AddCommand(cmd)

	// Users
	cmd = &cobra.Command{
		Use:   "create-user email",
		Short: "Create a user",
		Args:  cobra.ExactArgs(1),
		Run:   createUser}
	cmd.Flags().StringArray("role", nil, "user roles")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "delete-user user-id",
		Short: "Delete a user",
		Args:  cobra.ExactArgs(1),
		Run:   deleteUser}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "disable-user user-id",
		Short: "Disable a user",
		Args:  cobra.ExactArgs(1),
		Run:   disableUser}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "enable-user user-id",
		Short: "Enable a user",
		Args:  cobra.ExactArgs(1),
		Run:   enableUser}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "get-user user-id",
		Short: "Get information about the user with the given user-id",
		Args:  cobra.ExactArgs(1),
		Run:   getUser}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "find-user email",
		Short: "Find the user with the given email address",
		Args:  cobra.ExactArgs(1),
		Run:   findUser}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "list-users",
		Short: "List all users",
		Run:   listUsers}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "update-user user-id",
		Short: "Update a user",
		Args:  cobra.ExactArgs(1),
		Run:   updateUser}
	cmd.Flags().String("status", "", "user status")
	cmd.Flags().StringArray("role", nil, "user roles")
	root.AddCommand(cmd)

	// Snowflake integrations
	cmd = &cobra.Command{
		Use:   "create-snowflake-integration name",
		Short: "Create a Snowflake integration",
		Args:  cobra.ExactArgs(1),
		Run:   createSnowflakeIntegration}
	cmd.Flags().String("account", "", "Snowflake account (default: SNOWSQL_ACCOUNT env var)")
	cmd.Flags().String("admin-username", "", "Snowflake admin username (default: SNOWSQL_USER env var")
	cmd.Flags().String("admin-password", "", "Snowflake admin password (default: SNOWSQL_PWD env var")
	cmd.Flags().String("proxy-username", "", "Snowflake proxy username (default: SNOWSQL_USER env var")
	cmd.Flags().String("proxy-password", "", "Snowflake proxy password (default: SNOWSQL_PWD env var)")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "delete-snowflake-integration name",
		Short: "Delete a Snowflake integration",
		Args:  cobra.ExactArgs(1),
		Run:   deleteSnowflakeIntegration}
	cmd.Flags().String("username", "", "Snowflake username (default: SNOWSQL_USER env var)")
	cmd.Flags().String("password", "", "Snowflake password (default: SNOWSQL_PWD env var")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "get-snowflake-integration name",
		Short: "Get information about the given Snowflake integration",
		Args:  cobra.ExactArgs(1),
		Run:   getSnowflakeIntegration}
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "list-snowflake-integrations",
		Short: "List all Snowflake integrations",
		Args:  cobra.ExactArgs(0),
		Run:   listSnowflakeIntegrations}
	root.AddCommand(cmd)

	// Snowflake database links
	cmd = &cobra.Command{
		Use:   "create-snowflake-database-link integration",
		Short: "Create a Snowflake database link",
		Args:  cobra.ExactArgs(1),
		Run:   createSnowflakeDatabaseLink}
	cmd.Flags().String("database", "", "Snowflake database (default: SNOWSQL_DATABASE)")
	cmd.Flags().String("schema", "", "Snowflake schema (default: SNOWSQL_SCHEMA)")
	cmd.Flags().String("role", "", "Snowflake role (default: SNOWSQL_ROLE)")
	cmd.Flags().String("username", "", "Snowflake username (default: SNOWSQL_USER)")
	cmd.Flags().String("password", "", "Snowflake password (default: SNOWSQL_PWD)")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "delete-snowflake-database-link integration",
		Short: "Delete a Snowflake database link",
		Args:  cobra.ExactArgs(1),
		Run:   deleteSnowflakeDatabaseLink}
	cmd.Flags().String("database", "", "Snowflake database (default: SNOWSQL_DATABASE env var)")
	cmd.Flags().String("schema", "", "Snowflake schema (default: SNOWSQL_SCHEMA env var)")
	cmd.Flags().String("role", "", "Snowflake role (default: SNOWSQL_ROLE env var)")
	cmd.Flags().String("username", "", "Snowflake username (default: SNOWSQL_USER env var)")
	cmd.Flags().String("password", "", "Snowflake password (default: SNOWSQL_PWD env var)")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "get-snowflake-database-link integration",
		Short: "Get information about the given Snowflake database link",
		Args:  cobra.ExactArgs(1),
		Run:   getSnowflakeDatabaseLink}
	cmd.Flags().String("database", "", "Snowflake database (default: SNOWSQL_DATABASE env var)")
	cmd.Flags().String("schema", "", "Snowflake schema (default: SNOWSQL_SCHEMA env var)")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "list-snowflake-database-links integration",
		Short: "List Snowflake database links for the given integration",
		Args:  cobra.ExactArgs(1),
		Run:   listSnowflakeDatabaseLinks}
	root.AddCommand(cmd)

	// Snowflake Data Streams
	cmd = &cobra.Command{
		Use:   "create-snowflake-data-streams integration objectName",
		Short: "Create Snowflake data stream associated with an integration",
		Args:  cobra.ExactArgs(2),
		Run:   createSnowflakeDatastream}
	cmd.Flags().String("database", "", "Snowflake linked database (default: SNOWSQL_DATABASE env var)")
	cmd.Flags().String("schema", "", "Snowflake linked schema (default: SNOWSQL_SCHEMA env var)")
	cmd.Flags().String("role", "", "Snowflake role (default: SNOWSQL_ROLE env var)")
	cmd.Flags().String("warehouse", "", "Snowflake warehouse (default: SNOWSQL_WAREHOUSE env var)")
	cmd.Flags().String("username", "", "Snowflake username (default: SNOWSQL_USER env var)")
	cmd.Flags().String("password", "", "Snowflake password (default: SNOWSQL_PWD env var)")
	cmd.Flags().Bool("is-view", false, "Snowflake object is a view (default: false)")
	cmd.Flags().String("rai-database", "", "RelationAI target database name")
	cmd.MarkFlagRequired("rai-database")
	cmd.Flags().String("relation", "", "RelationAI relation")
	cmd.MarkFlagRequired("relation")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "delete-snowflake-data-stream integration objectName",
		Short: "Delete a Snowflake data stream associated with an integration",
		Args:  cobra.ExactArgs(2),
		Run:   deleteSnowflakeDatastream}
	cmd.Flags().String("database-link", "", "Integration databse-link name (required)")
	cmd.MarkFlagRequired("database-link")
	cmd.Flags().String("role", "", "Snowflake role (default: SNOWSQL_ROLE env var)")
	cmd.Flags().String("warehouse", "", "Snowflake warehouse (default: SNOWSQL_WAREHOUSE env var)")
	cmd.Flags().String("username", "", "Snowflake username (default: SNOWSQL_USER env var)")
	cmd.Flags().String("password", "", "Snowflake password (default: SNOWSQL_PWD env var)")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "get-snowflake-data-stream integration objectName",
		Short: "Get information about a Snowflake data stream associated with an integration",
		Args:  cobra.ExactArgs(2),
		Run:   getSnowflakeDatastream}
	cmd.Flags().String("database-link", "", "Integration databse-link name (required)")
	cmd.MarkFlagRequired("database-link")
	root.AddCommand(cmd)

	cmd = &cobra.Command{
		Use:   "list-snowflake-data-streams integration",
		Short: "List Snowflake data streams associated with an integration ",
		Args:  cobra.ExactArgs(1),
		Run:   listSnowflakeDatastreams}
	cmd.Flags().String("database-link", "", "Integration databse-link name (required)")
	cmd.MarkFlagRequired("database-link")
	root.AddCommand(cmd)

	// Misc
	cmd = &cobra.Command{
		Use:   "get-access-token",
		Short: "Get OAuth access token",
		Run:   getAccessToken}
	root.AddCommand(cmd)
}

func main() {
	var root = &cobra.Command{Use: "rai"}
	root.PersistentFlags().String("host", "", "host name")
	root.PersistentFlags().String("port", "", "port number")
	root.PersistentFlags().String("config", "~/.rai/config", "config file")
	root.PersistentFlags().String("profile", "default", "config profile")
	root.PersistentFlags().BoolP("quiet", "q", false, "silence status output")
	root.PersistentFlags().String("format", "pretty", "format results, 'json' or 'pretty'")
	addCommands(root)
	root.Execute()
}
