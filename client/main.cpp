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
#include <crossbow/program_options.hpp>
#include <crossbow/logger.hpp>

#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <string>
#include <iostream>

#include "Client.hpp"

using namespace crossbow::program_options;
using namespace boost::asio;
using err_code = boost::system::error_code;

int main(int argc, const char** argv) {
    bool help = false;
    bool populate = false;
    int16_t numWarehouses = 1;
    std::string host;
    std::string port("8713");
    std::string logLevel("DEBUG");
    size_t numClients = 1;
    auto opts = create_options("tpcc_server",
            value<'h'>("help", &help, tag::description{"print help"})
            , value<'H'>("host", &host, tag::description{"Host to bind to"})
            , value<'p'>("port", &port, tag::description{"Port to bind to"})
            , value<'l'>("log-level", &logLevel, tag::description{"The log level"})
            , value<'c'>("num-clients", &numClients, tag::description{"Number of Clients to run"})
            , value<'P'>("populate", &populate, tag::description{"Populate the database"})
            , value<'W'>("num-warehouses", &numWarehouses, tag::description{"Number of warehouses"})
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
    if (host.empty()) {
        std::cerr << "No host\n";
        return 1;
    }
    std::vector<tpcc::Client> clients;
    clients.reserve(numClients);
    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);
    try {
        io_service service;
        ip::tcp::resolver resolver(service);
        ip::tcp::resolver::iterator iter;
        if (host == "") {
            iter = resolver.resolve(ip::tcp::resolver::query(port));
        } else {
            iter = resolver.resolve(ip::tcp::resolver::query(host, port));
        }
        auto wareHousesPerClient = numWarehouses / numClients;
        for (decltype(numClients) i = 0; i < numClients; ++i) {
            clients.emplace_back(service);
            boost::asio::connect(clients[i].socket(), iter);
            LOG_INFO(("Connected to client" + crossbow::to_string(i)));
            if (!populate) {
                clients[i].run();
            }
        }
        if (populate) {
            auto& cmds = clients[0].commands();
            cmds.execute<tpcc::Command::CREATE_SCHEMA>(
                    [&clients, numClients, wareHousesPerClient, numWarehouses](const err_code& ec,
                        const std::tuple<bool, crossbow::string>& res){
                if (ec) {
                    LOG_ERROR(ec.message());
                    return;
                }
                if (!std::get<0>(res)) {
                    LOG_ERROR(std::get<1>(res));
                    return;
                }
                for (decltype(numClients) i = 0; i < numClients; ++i) {
                    auto upper = int16_t(int16_t(wareHousesPerClient * (i + 1)));
                    if (i + 1 == numClients) {
                        upper = numWarehouses;
                    }
                    clients[i].populate(int16_t((wareHousesPerClient * i) + 1), upper);
                }
            });
        }
        service.run();
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}
