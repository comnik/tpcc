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
#include <cassert>
#include <fstream>

#include <common/Util.hpp>

#include "Client.hpp"

using namespace crossbow::program_options;
using namespace boost::asio;
using err_code = boost::system::error_code;

int main(int argc, const char** argv) {
    bool help = false;
    bool populate = false;
    bool useCHTables = false;
    int16_t numWarehouses = 1;
    crossbow::string host;
    std::string port("8713");
    std::string logLevel("DEBUG");
    std::string outFile("out.csv");
    size_t numClients = 1;
    unsigned time = 5*60;
    bool exit = false;
    auto opts = create_options("tpcc_server",
            value<'h'>("help", &help, tag::description{"print help"})
            , value<'H'>("host", &host, tag::description{"Comma-separated list of hosts"})
            , value<'l'>("log-level", &logLevel, tag::description{"The log level"})
            , value<'c'>("num-clients", &numClients, tag::description{"Number of Clients to run per host"})
            , value<'P'>("populate", &populate, tag::description{"Populate the database"})
            , value<'W'>("num-warehouses", &numWarehouses, tag::description{"Number of warehouses"})
            , value<'t'>("time", &time, tag::description{"Duration of the benchmark in seconds"})
            , value<'o'>("out", &outFile, tag::description{"Path to the output file"})
            , value<-1>("exit", &exit, tag::description{"Quit server"})
            , value<'a'>("ch-bench-analytics", &useCHTables,
                         tag::description{"Populate the database witht he additional tables used in the CHBenchmark"})
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
    auto startTime = tpcc::Clock::now();
    auto endTime = startTime + std::chrono::seconds(time);
    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);
    try {
        auto hosts = tpcc::split(host.c_str(), ',');
        io_service service;
        auto sumClients = hosts.size() * numClients;
        std::vector<tpcc::Client> clients;
        clients.reserve(sumClients);
        auto wareHousesPerClient = numWarehouses / sumClients;
        for (decltype(sumClients) i = 0; i < sumClients; ++i) {
            if (i >= unsigned(numWarehouses)) break;
            clients.emplace_back(service, numWarehouses, int16_t(wareHousesPerClient * i + 1), int16_t(wareHousesPerClient * (i + 1)), endTime);
        }
        for (size_t i = 0; i < hosts.size(); ++i) {
            auto h = hosts[i];
            auto addr = tpcc::split(h, ':');
            assert(addr.size() <= 2);
            auto p = addr.size() == 2 ? addr[1] : port;
            ip::tcp::resolver resolver(service);
            ip::tcp::resolver::iterator iter;
            if (host == "") {
                iter = resolver.resolve(ip::tcp::resolver::query(p));
            } else {
                iter = resolver.resolve(ip::tcp::resolver::query(addr[0], p));
            }
            for (unsigned j = 0; j < numClients; ++j) {
                LOG_INFO("Connected to client " + crossbow::to_string(i*numClients + j));
                boost::asio::connect(clients[i*numClients + j].socket(), iter);
            }
        }

        {
            auto t = std::time(nullptr);
            std::string dateString(20, '\0');
            auto len = std::strftime(&dateString.front(), 20, "%T", std::localtime(&t));
            dateString.resize(len);
            LOG_INFO("Starting client at %1%", dateString);
        }

        if (exit) {
            for (auto& client : clients) {
                auto& cmds = client.commands();
                cmds.execute<tpcc::Command::EXIT>([](const err_code& ec){
                    if (ec) {
                        std::cerr << "ERROR: " << ec.message() << std::endl;
                    }
                });
                goto END;
            }
        }

        if (populate) {
            auto& cmds = clients[0].commands();
            cmds.execute<tpcc::Command::CREATE_SCHEMA>(
                    [&clients, &useCHTables](const err_code& ec,
                        const std::tuple<bool, crossbow::string>& res){
                if (ec) {
                    LOG_ERROR(ec.message());
                    return;
                }
                if (!std::get<0>(res)) {
                    LOG_ERROR(std::get<1>(res));
                    return;
                }

                auto& cmds = clients[0].commands();
                cmds.execute<tpcc::Command::POPULATE_DIM_TABLES>([&clients, &useCHTables](const err_code& ec, const std::tuple<bool, crossbow::string>& res){
                    if (ec) {
                        LOG_ERROR(ec.message());
                        return;
                    }
                    if (!std::get<0>(res)) {
                        LOG_ERROR(std::get<1>(res));
                        return;
                    }

                    for (auto& client : clients) {
                        client.populate(useCHTables);
                    }
                }, useCHTables);
            }, useCHTables);
        } else {
            for (decltype(clients.size()) i = 0; i < clients.size(); ++i) {
                auto& client = clients[i];
                client.run();
            }
        }
END:
        service.run();
        LOG_INFO("Done, writing results");
        std::ofstream out(outFile.c_str());
        out << "start,end,transaction,success,error\n";
        for (const auto& client : clients) {
            const auto& queue = client.log();
            for (const auto& e : queue) {
                crossbow::string tName;
                switch (e.transaction) {
                case tpcc::Command::POPULATE_WAREHOUSE:
                    tName = "Populate";
                    break;
                case tpcc::Command::POPULATE_DIM_TABLES:
                    tName = "Populate";
                    break;
                case tpcc::Command::CREATE_SCHEMA:
                    tName = "Schema Create";
                    break;
                case tpcc::Command::STOCK_LEVEL:
                    tName = "Stock Level";
                    break;
                case tpcc::Command::DELIVERY:
                    tName = "Delivery";
                    break;
                case tpcc::Command::NEW_ORDER:
                    tName = "New Order";
                    break;
                case tpcc::Command::ORDER_STATUS:
                    tName = "Order Status";
                    break;
                case tpcc::Command::PAYMENT:
                    tName = "Payment";
                    break;
                case tpcc::Command::EXIT:
                    assert(false);
                    break;
                }
                out << std::chrono::duration_cast<std::chrono::milliseconds>(e.start - startTime).count() << ','
                    << std::chrono::duration_cast<std::chrono::milliseconds>(e.end - startTime).count() << ','
                    << tName << ','
                    << (e.success ? "true" : "false") << ','
                    << e.error << std::endl;
            }
        }
        std::cout << '\a';
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}
