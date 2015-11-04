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
#include "CreateSchema.hpp"

using namespace tell::db;

namespace tpcc {

DeliveryResult Transactions::delivery(Transaction& tx, const DeliveryIn& in) {
    DeliveryResult result;
    try {
        auto noTableF = tx.openTable("new-order");
        auto oTableF = tx.openTable("order");
        auto olTableF = tx.openTable("order-line");
        auto cTableF = tx.openTable("customer");
        auto cTable = cTableF.get();
        auto olTable = olTableF.get();
        auto oTable = oTableF.get();
        auto noTable = noTableF.get();
        auto ol_delivery_d = now();
        for (int16_t d_id = 1; d_id <= 10; ++d_id) {
            auto iter = tx.lower_bound(noTable, "new-order-idx", {
                    Field::create(in.w_id),
                    Field::create(d_id),
                    Field::create(int32_t(0))});
            if (iter.done()) continue;
            NewOrderKey noKey{iter.value()};
            if (noKey.w_id != in.w_id || noKey.d_id != d_id) continue;
            OrderKey oKey{in.w_id, d_id, noKey.o_id};
            auto newOrderF = tx.get(noTable, noKey.key());
            auto orderF = tx.get(oTable, oKey.key());
            auto order = orderF.get();
            auto nOrder = order;

            auto newOrder = newOrderF.get();
            tx.remove(noTable, noKey.key(), newOrder);
            nOrder.at("o_carrier_id") = Field::create(in.o_carrier_id);
            tx.update(oTable, oKey.key(), order, nOrder);
            auto o_ol_cnt = boost::any_cast<int16_t>(order.at("o_ol_cnt").value());
            std::vector<Future<Tuple>> orderLinesF;
            orderLinesF.reserve(o_ol_cnt);
            std::vector<tell::db::key_t> ol_keys;
            ol_keys.reserve(o_ol_cnt);
            for (decltype(o_ol_cnt) ol_number = 1; ol_number <= o_ol_cnt; ++ol_number) {
                OrderlineKey olKey(in.w_id, d_id, oKey.o_id, ol_number);
                auto k = olKey.key();
                orderLinesF.emplace_back(tx.get(olTable, k));
                ol_keys.emplace_back(k);
            }
            CustomerKey cKey{in.w_id, d_id, boost::any_cast<int16_t>(order.at("o_c_id").value())};
            auto customerF = tx.get(cTable, cKey.key());
            auto customer = customerF.get();
            int64_t amount = 0;
            for (size_t i = orderLinesF.size(); i > 0; ++i) {
                auto orderline = orderLinesF[i - 1].get();
                auto nOrderline = orderline;
                amount += boost::any_cast<int32_t>(orderline.at("ol_amount").value());
                nOrderline.at("ol_delivery_d") = Field::create(ol_delivery_d);
                tx.update(olTable, ol_keys[i - 1], orderline, nOrderline);
            }
            auto nCustomer = customer;
            nCustomer.at("c_balance") += Field::create(amount);
            nCustomer.at("c_delivery_cnt") += Field::create(int16_t(1));
            tx.update(cTable, cKey.key(), customer, nCustomer);
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

