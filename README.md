# tpcc
This is a TPC-C implementation to benchmark TellStore and Kudu.

## Building
First, build [Tell](https://github.com/tellproject/tell). Then, clone then newest version of the TPC-C benchmark and build Tell again. This will also build the TPC-C benchmark:

```bash
cd <tell-main-directory>
cd watch
git clone https://github.com/tellproject/tpcc.git
cd <tell-build>
make
```

### Building for Kudu
If you want to run the TPC-C Benchmark not only on Tell, but also on [Kudu](http://getkudu.io), first make sure that you configure the kuduClient_DIR correctly in the CMakeLists.txt. Once it is set correctly, you have to call cmake again in the tell build directory and set the additional flag:

```bash
-DUSE_KUDU=ON
```

## Running
The simplest way to run the benchmark is to use the [Python Helper Scripts](https://github.com/tellproject/helper_scripts). They will not only help you to start TellStore, but also one or several TPC-C servers.

### Server
Of course, you can also run the server from the commandline. Depending on whether you want to use Tell or Kudu as the storage backend, execute one of the two lines below. This will print out to the console the commandline options you need to start the server:

```bash
watch/tpcc/tpcc_server -h
watch/tpcc/tpcc_kudu -h
```

### Client
The TPC-C client uses a TCP connection to send transaction requests to a TPC-C server. It writes a log file in CSV format where it logs every transaction that was executed with transaction type, start time, end time (both in millisecs and relative to the beginning of the experiment) as well as whether the transaction was successfully commited or not. This log file can then be grepped in order to compute some other useful statistics (like e.g. TpmC). The client can connect to server(s) regardless of the used storage backend. You can find out about the commandline options for the client by typing:

```bash
watch/tpcc/tpcc_client -h
```
