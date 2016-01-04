/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#include <string>
#include <thread>
#include <boost/asio.hpp>
#include <crossbow/program_options.hpp>
#include <crossbow/logger.hpp>

#include <kudu/client/client.h>

#include <common/Protocol.hpp>
#include "kudu.hpp"
#include "CreateSchemaKudu.hpp"
#include "PopulateKudu.hpp"

using namespace crossbow::program_options;
using namespace boost::asio;

namespace tpcc {

void assertOk(kudu::Status status) {
    if (!status.ok()) {
        LOG_ERROR("ERROR from Kudu: %1%", status.message().ToString());
    }
}

using Session = std::tr1::shared_ptr<kudu::client::KuduSession>;

class Connection {
    boost::asio::ip::tcp::socket mSocket;
    server::Server<Connection> mServer;
    Session mSession;
    Populator mPopulator;
public:
    Connection(boost::asio::io_service& service, kudu::client::KuduClient& client, int16_t numWarehouses)
        : mSocket(service)
        , mServer(*this, mSocket)
        , mSession(client.NewSession())
    {
        assertOk(mSession->SetFlushMode(kudu::client::KuduSession::MANUAL_FLUSH));
        mSession->SetTimeoutMillis(60000);
    }
    ~Connection() = default;
    decltype(mSocket)& socket() { return mSocket; }
    void run() {
        mServer.run();
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::EXIT, void>::type
    execute(const Callback callback) {
        mServer.quit();
        callback();
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::CREATE_SCHEMA, void>::type
    execute(bool args, const Callback& callback) {
        createSchema(*mSession, args);
        callback(std::make_tuple(true, crossbow::string()));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::POPULATE_WAREHOUSE, void>::type
    execute(std::tuple<int16_t, bool> args, const Callback& callback) {
        mPopulator.populateWarehouse(*mSession, std::get<0>(args), std::get<1>(args));
        callback(std::make_tuple(true, crossbow::string()));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::POPULATE_DIM_TABLES, void>::type
    execute(bool args, const Callback& callback) {
        mPopulator.populateDimTables(*mSession, args);
        callback(std::make_tuple(true, crossbow::string()));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::NEW_ORDER, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::PAYMENT, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::ORDER_STATUS, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::DELIVERY, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::STOCK_LEVEL, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
    }
};

void accept(io_service& service, ip::tcp::acceptor& a, kudu::client::KuduClient& client, int16_t numWarehouses) {
    auto conn = new Connection(service, client, numWarehouses);
    a.async_accept(conn->socket(), [&, conn, numWarehouses](const boost::system::error_code& err) {
        if (err) {
            delete conn;
            LOG_ERROR(err.message());
            return;
        }
        conn->run();
        accept(service, a, client, numWarehouses);
    });
}

}

int main(int argc, const char* argv[]) {
    bool help = false;
    std::string host;
    std::string port("8713");
    std::string logLevel("DEBUG");
    crossbow::string storageNodes;
    int16_t numWarehouses = 0;
    unsigned numThreads = 1;
    auto opts = create_options("tpcc_server",
            value<'h'>("help", &help, tag::description{"print help"}),
            value<'H'>("host", &host, tag::description{"Host to bind to"}),
            value<'p'>("port", &port, tag::description{"Port to bind to"}),
            value<'l'>("log-level", &logLevel, tag::description{"The log level"}),
            value<'s'>("storage-nodes", &storageNodes, tag::description{"Semicolon-separated list of storage node addresses"}),
            value<'W'>("num-warehouses", &numWarehouses, tag::description{"Number of warehouses"}),
            value<-1>("network-threads", &numThreads, tag::ignore_short<true>{})
            );
    try {
        parse(opts, argc, argv);
    } catch (argument_not_found& e) {
        std::cerr << e.what() << std::endl << std::endl;
        print_help(std::cout, opts);
        return 1;
    }
    if (help) {
        print_help(std::cout, opts);
        return 0;
    }
    if (numWarehouses == 0) {
        std::cerr << "Number of warehouses needs to be set" << std::endl;
        return 1;
    }

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);
    try {
        io_service service;
        boost::asio::io_service::work work(service);
        ip::tcp::acceptor a(service);
        boost::asio::ip::tcp::acceptor::reuse_address option(true);
        ip::tcp::resolver resolver(service);
        ip::tcp::resolver::iterator iter;
        if (host == "") {
            iter = resolver.resolve(ip::tcp::resolver::query(port));
        } else {
            iter = resolver.resolve(ip::tcp::resolver::query(host, port));
        }
        ip::tcp::resolver::iterator end;
        for (; iter != end; ++iter) {
            boost::system::error_code err;
            auto endpoint = iter->endpoint();
            auto protocol = iter->endpoint().protocol();
            a.open(protocol);
            a.set_option(option);
            a.bind(endpoint, err);
            if (err) {
                a.close();
                LOG_WARN("Bind attempt failed " + err.message());
                continue;
            }
            break;
        }
        if (!a.is_open()) {
            LOG_ERROR("Could not bind");
            return 1;
        }
        a.listen();
        // Connect to Kudu
        kudu::client::KuduClientBuilder clientBuilder;
        clientBuilder.add_master_server_addr(storageNodes.c_str());
        std::tr1::shared_ptr<kudu::client::KuduClient> client;
        tpcc::assertOk(clientBuilder.Build(&client));
        // we do not need to delete this object, it will delete itself
        tpcc::accept(service, a, *client, numWarehouses);
        std::vector<std::thread> threads;
        for (unsigned i = 0; i < numThreads; ++i) {
            threads.emplace_back([&service](){
                    service.run();
            });
        }
        for (auto& t : threads) {
            t.join();
        }
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}
