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
#include "Client.hpp"
#include <common/Protocol.hpp>
#include <crossbow/logger.hpp>

using err_code = boost::system::error_code;

namespace tpcc {

void Client::run() {
}

void Client::populate(int16_t lower, int16_t upper) {
    mCmds.execute<Command::POPULATE_WAREHOUSE>(
            [this, lower, upper](const err_code& ec, const std::tuple<bool, crossbow::string>& res){
                if (ec) {
                    LOG_ERROR(ec.message());
                    return;
                }
                if (std::get<0>(res)) {
                    LOG_ERROR(std::get<1>(res));
                    return;
                }
                LOG_INFO(("Populated Warehouse " + crossbow::to_string(lower)));
                if (lower == upper) return; // population done
                populate(lower + 1, upper);
            },
            lower);
}

} // namespace tpcc

