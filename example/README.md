# Examples

## setup

before using the example, you'll need to install the Big Table Emulator as documented [here](https://cloud.google.com/bigtable/docs/emulator).

You'll need to run the emulator with the following command:

```shell
gcloud beta emulators bigtable start
```

Keep it running and in a separate terminal window, run the following command to set up the environment to use the emulator:

```shell
export BIGTABLE_EMULATOR_HOST=127.0.0.1:8086
```

(caution: host must be `127.0.0.1`, not `localhost`)

Now, it's time to create your first table with its column family.

```shell
 cbt -project example-project -instance example-instance createtable ecommerce_events
```

```shell
cbt -project example-project -instance example-instance createfamily ecommerce_events front
```
 now let's check that everything's fine
 
```shell
cbt -project example-project -instance example-instance ls ecommerce_events 
```

Alternatively, you can use the `create-table.sh` script to create the table.

```shell
./create-table.sh
```
and you're set.

## usage

just run `main`

```shell
go run main.go
```

it will insert some data in the table and then print the result of two `count` aggregations + two mapped events. Feel free to edit the code to see how it works and run some tests on your own.
