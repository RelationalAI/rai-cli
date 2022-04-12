# The RelationalAI Command Line Interface (CLI)

The RelationalAI (RAI) CLI provides access to the RAI APIs from the command line.

* You can find RelationalAI product documentation at <https://docs.relational.ai>
* You can learn more about RelationalAI at <https://relational.ai>

## Getting started

### Building the CLI

Update dependencies with the  `tidy` script

    ./tidy

Build the `rai` binary using the `go` tool

    go build -o build/rai rai/*o

.. or use the handy `make` script

    ./make

Installation consists of simply putting the `rai` binary on your path.

### Create a configuration file

In order to use the CLI, you will need to create config file. The default location
for the file is `$HOME/.rai/config` and the file should include the following:

Sample configuration using OAuth client credentials:

    [default]
    host = azure.relationalai.com
    port = <api-port>      # optional, default: 443
    scheme = <scheme>      # optional, default: https
    client_id = <your client_id>
    client_secret = <your client secret>
    client_credentials_url = <account login URL>  # optional
    # default: https://login.relationalai.com/oauth/token

Client credentials can be created using the RAI console at
https://console.relationalai.com/login

You can copy `config.spec` from the root of this repo and modify as needed.

### Running the tests

    cd ./test
    ./run-all

## Support

You can reach the RAI developer support team at `support@relational.ai`

## Contributing

We value feedback and contributions from our developer community. Feel free
to submit an issue or a PR here.

## License

The RelationalAI Command Line Interface is licensed under the Apache License 2.0. See:
https://github.com/RelationalAI/rai-sdk-java/blob/master/LICENSE
