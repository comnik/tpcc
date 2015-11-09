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
#include "Transactions.hpp"
#include <telldb/Exceptions.hpp>
#include <sstream>

using namespace tell::db;

namespace tpcc {

OrderStatusResult Transactions::orderStatus(Transaction& tx, const OrderStatusIn& in) {
    OrderStatusResult result;
    try {
        auto oTableF = tx.openTable("order");
        auto olTableF = tx.openTable("order-line");
        auto cTableF = tx.openTable("customer");
        auto cTable = cTableF.get();
        auto oTable = oTableF.get();
        auto olTable = olTableF.get();
        // get Customer
        CustomerKey cKey{0, 0, 0};
        auto customerF = getCustomer(tx, in.selectByLastName, in.c_last, in.w_id, in.d_id, in.c_id, cTable, cKey);
        // get newest order
        auto iter = tx.reverse_lower_bound(oTable, "order_idx", {
                Field(in.w_id)
                , Field(in.d_id)
                , Field(cKey.c_id)
                , Field(std::numeric_limits<int32_t>::max())
                });
        if (iter.done()) {
            result.success = false;
            std::stringstream errstream;
            errstream << "Customer name=" << in.c_last << ", w_id=" << in.w_id << ", d_id=" << in.d_id << ", c_id="
                    << cKey.c_id << " does not exist";
            result.error = errstream.str();
            return result;
        }
        OrderKey oKey{iter.value()};
        auto orderF = tx.get(oTable, iter.value());
        auto order = orderF.get();
        auto customer = customerF.get();
        auto ol_cnt = order.at("o_ol_cnt").value<int16_t>();
        // To get the order lines, we could use an index - but this is not necessary,
        // since we can generate all primary keys instead
        OrderlineKey olKey{in.w_id, in.d_id, oKey.o_id, int16_t(1)};
        std::vector<Future<Tuple>> reqs;
        reqs.reserve(ol_cnt);
        for (decltype(ol_cnt) i = 1; i <= ol_cnt; ++i) {
            olKey.ol_number = i;
            reqs.emplace_back(tx.get(olTable, olKey.key()));
        }
        for (auto& f : reqs) {
            f.get();
        }
        tx.commit();
        result.success = true;
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }
    return result;
}

} // namespace tpcc

