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

using namespace tell::db;

namespace tpcc {

StockLevelResult Transactions::stockLevel(Transaction& tx, const StockLevelIn& in) {
    StockLevelResult result;
    try {
        auto dTableF = tx.openTable("district");
        auto oTableF = tx.openTable("order");
        auto olTableF = tx.openTable("order-line");
        auto sTableF = tx.openTable("stock");
        auto sTable = sTableF.get();
        auto olTable = olTableF.get();
        auto oTable = oTableF.get();
        auto dTable = dTableF.get();

        // get District
        DistrictKey dKey{in.w_id, in.d_id};
        auto districtF = tx.get(dTable, dKey.key());
        auto district = districtF.get();
        auto d_next_o_id = district.at("d_next_o_id").value<int32_t>();
        OrderKey oKey{in.w_id, in.d_id, 0};
        // get the 20 newest orders - this is not required in the benchmark,
        // but it allows us to not use an index
        std::vector<std::pair<int32_t, Future<Tuple>>> ordersF;
        ordersF.reserve(20);
        for (decltype(d_next_o_id) ol_o_id = d_next_o_id - 20; ol_o_id < d_next_o_id; ++ol_o_id) {
            oKey.o_id = ol_o_id;
            ordersF.emplace_back(ol_o_id, tx.get(oTable, oKey.key()));
        }
        // get the order-lines
        std::vector<Future<Tuple>> orderlinesF;
        OrderlineKey olKey{in.w_id, in.d_id, 0, 0};
        for (auto& orderF : ordersF) {
            olKey.o_id = orderF.first;
            auto order = orderF.second.get();
            auto o_ol_cnt = order.at("o_ol_cnt").value<int16_t>();
            for (decltype(o_ol_cnt) ol_number = 1; ol_number <= o_ol_cnt; ++ol_number) {
                olKey.ol_number = ol_number;
                orderlinesF.emplace_back(tx.get(olTable, olKey.key()));
            }
        }
        result.low_stock = 0;
        // count low_stock
        std::unordered_map<int32_t, Future<Tuple>> stocksF;
        for (auto& olF : orderlinesF) {
            auto ol = olF.get();
            auto ol_i_id = ol.at("ol_i_id").value<int32_t>();
            if (stocksF.count(ol_i_id) == 0) {
                stocksF.emplace(ol_i_id, tx.get(sTable, tell::db::key_t{(uint64_t(in.w_id) << 4*8) | uint64_t(ol_i_id)}));
            }
        }
        for (auto& p : stocksF) {
            auto stock = p.second.get();
            auto quantity = stock.at("s_quantity").value<int32_t>();
            if (quantity < in.threshold) {
                ++result.low_stock;
            }
        }
        tx.commit();
        result.success = true;
        return result;
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }
    return result;
}

} // namespace tpcc

