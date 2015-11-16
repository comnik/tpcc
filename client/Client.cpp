/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and
 * others.
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

template <Command C>
void Client::execute(const typename Signature<C>::arguments &arg) {
    auto now = Clock::now();
    if (now > mEndTime) {
        // Time's up
        // benchmarking finished
        mSocket.shutdown(Socket::shutdown_both);
        mSocket.close();
        return;
    }
    mCmds.execute<C>(
      [this, now](const err_code &ec, typename Signature<C>::result result) {
          if (ec) {
              LOG_ERROR("Error: " + ec.message());
              return;
          }
          auto end = Clock::now();
          if (!result.success) {
              LOG_ERROR("Transaction unsuccessful [error = %1%]", result.error);
          }
          mLog.push_back(LogEntry{result.success, result.error, C, now, end});
          run();
      },
      arg);
}

void Client::run() {
    auto n = rnd.random<int>(1, 100);
    if (n <= 4) {
        LOG_DEBUG("Start stock-level Transaction");
        StockLevelIn args;
        args.w_id      = mCurrWarehouse;
        args.d_id      = mCurrDistrict;
        args.threshold = rnd.randomWithin<int32_t>(10, 20);
        execute<Command::STOCK_LEVEL>(args);
        mCurrDistrict = mCurrDistrict == 10 ? 1 : (mCurrDistrict + 1);
    } else if (n <= 8) {
        LOG_DEBUG("Start delivery Transaction");
        DeliveryIn arg;
        arg.w_id         = mCurrWarehouse;
        arg.o_carrier_id = rnd.random<int16_t>(1, 10);
        execute<Command::DELIVERY>(arg);
    } else if (n <= 12) {
        LOG_DEBUG("Start order-status Transaction");
        OrderStatusIn arg;
        arg.w_id             = mCurrWarehouse;
        arg.d_id             = rnd.random<int16_t>(1, 10);
        arg.selectByLastName = 6 <= rnd.random<int>(1, 10);
        if (arg.selectByLastName) {
            arg.c_last = rnd.cLastName(rnd.NURand(255, 0, 999));
        } else {
            arg.c_id = rnd.NURand<int32_t>(1023, 1, 3000);
        }
        execute<Command::ORDER_STATUS>(arg);
    } else if (n <= 55) {
        LOG_DEBUG("Start payment Transaction");
        PaymentIn arg;
        arg.w_id = mCurrWarehouse;
        arg.d_id = rnd.random<int16_t>(1, 10);
        auto x   = rnd.random(1, 100);
        if (x <= 85) {
            arg.c_w_id = mCurrWarehouse;
            arg.c_d_id = arg.d_id;
        } else {
            arg.c_w_id = rnd.random<int16_t>(1, mNumWarehouses);
            arg.c_d_id = rnd.random<int16_t>(1, 10);
        }
        auto y = rnd.random(1, 100);
        arg.selectByLastName = y <= 60;
        if (arg.selectByLastName) {
            arg.c_last = rnd.cLastName(rnd.NURand(255, 0, 999));
        } else {
            arg.c_id = rnd.NURand<int32_t>(1023, 1, 3000);
        }
        arg.h_amount = rnd.random<int32_t>(100, 500000);
        execute<Command::PAYMENT>(arg);
    } else {
        LOG_DEBUG("Start new-order Transaction");
        NewOrderIn arg;
        arg.w_id = mCurrWarehouse;
        arg.d_id = rnd.random<int16_t>(1, 10);
        arg.c_id = rnd.NURand<int32_t>(1023, 1, 3000);
        execute<Command::NEW_ORDER>(arg);
    }
    mCurrWarehouse = mCurrWarehouse == mWareHouseUpper ? mWareHouseLower
                                                       : (mCurrWarehouse + 1);
}

void Client::populate(bool useCH) { populate(mWareHouseLower, mWareHouseUpper, useCH); }

void Client::populate(int16_t lower, int16_t upper, bool useCH) {
    mCmds.execute<Command::POPULATE_WAREHOUSE>(
      [this, lower, upper, useCH](const err_code &ec,
                           const std::tuple<bool, crossbow::string> &res) {
          if (ec) {
              LOG_ERROR(ec.message());
              return;
          }
          LOG_ASSERT(std::get<0>(res), std::get<1>(res));
          LOG_INFO(("Populated Warehouse " + crossbow::to_string(lower)));
          if (lower == upper) {
              mSocket.shutdown(Socket::shutdown_both);
              mSocket.close();
              return; // population done
          }
          populate(lower + 1, upper, useCH);
      },
      std::make_tuple(lower, useCH));
}

} // namespace tpcc
