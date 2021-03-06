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
#include "Connection.hpp"
#include <crossbow/allocator.hpp>
#include <crossbow/program_options.hpp>
#include <crossbow/logger.hpp>
#include <telldb/TellDB.hpp>

#include <boost/asio.hpp>
#include <string>
#include <iostream>

using namespace crossbow::program_options;
using namespace boost::asio;

void accept(boost::asio::io_service &service,
        boost::asio::ip::tcp::acceptor &a,
        tell::db::ClientManager<void>& clientManager,
        int16_t numWarehouses) {
    auto conn = new tpcc::Connection(service, clientManager, numWarehouses);
    a.async_accept(conn->socket(), [conn, &service, &a, &clientManager, numWarehouses](const boost::system::error_code &err) {
        if (err) {
            delete conn;
            LOG_ERROR(err.message());
            return;
        }
        conn->run();
        accept(service, a, clientManager, numWarehouses);
    });
}

int main(int argc, const char** argv) {
    bool help = false;
    std::string host;
    std::string port("8713");
    std::string logLevel("DEBUG");
    crossbow::string commitManager;

    tell::store::ClientConfig config;
    int16_t numWarehouses = 0;
    auto opts = create_options("tpcc_server",
            value<'h'>("help", &help, tag::description{"print help"}),
            value<'H'>("host", &host, tag::description{"Host to bind to"}),
            value<'p'>("port", &port, tag::description{"Port to bind to"}),
            value<'l'>("log-level", &logLevel, tag::description{"The log level"}),
            value<'c'>("commit-manager", &commitManager, tag::description{"Address to the commit manager"}),
            value<'W'>("num-warehouses", &numWarehouses, tag::description{"Number of warehouses"}),
            value<-1>("network-threads", &config.numNetworkThreads, tag::ignore_short<true>{})
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

    crossbow::allocator::init();

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);
    config.commitManager = config.parseCommitManager(commitManager);
    tell::db::ClientManager<void> clientManager(config);

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
        // we do not need to delete this object, it will delete itself
        accept(service, a, clientManager, numWarehouses);
        service.run();
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}
