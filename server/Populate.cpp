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
#include "Populate.hpp"
#include <telldb/Transaction.hpp>
#include <chrono>
#include <algorithm>

using namespace tell::db;

namespace tpcc {

void Populator::populateDimTables(Transaction &transaction, bool useCH)
{
    populateItems(transaction);
    if (useCH) {
        populateRegions(transaction);
        populateNations(transaction);
        populateSuppliers(transaction);
    }
}

void Populator::populateWarehouse(tell::db::Transaction &transaction,
                                  Counter &counter, int16_t w_id, bool useCH) {
    auto tIdFuture = transaction.openTable("warehouse");
    auto table = tIdFuture.get();
    tell::db::key_t key{uint64_t(w_id)};
    transaction.insert(table, key,
                       {{{"w_id", w_id},
                         {"w_name", mRandom.astring(6, 10)},
                         {"w_street_1", mRandom.astring(10, 20)},
                         {"w_street_2", mRandom.astring(10, 20)},
                         {"w_city", mRandom.astring(10, 20)},
                         {"w_state", mRandom.astring(10, 20)},
                         {"w_zip", mRandom.astring(2, 2)},
                         {"w_tax", mRandom.random<int32_t>(0, 2000)},
                         {"w_ytd", int64_t(30000000)}}});
    populateStocks(transaction, w_id, useCH);
    populateDistricts(transaction, counter, w_id, useCH);
}

void Populator::populateItems(tell::db::Transaction &transaction) {
    auto tIdFuture = transaction.openTable("item");
    auto tId = tIdFuture.get();
    for (int32_t i = 1; i <= 100000; ++i) {
        transaction.insert(
          tId, tell::db::key_t{uint64_t(i)},
          {{{"i_id", i},
            {"i_im_id", mRandom.randomWithin<int32_t>(1, 10000)},
            {"i_name", mRandom.astring(14, 24)},
            {"i_price", mRandom.randomWithin<int32_t>(100, 10000)},
            {"i_data", mRandom.astring(26, 50)}}});
    }
}

void Populator::populateRegions(Transaction &transaction)
{
    auto tIdFuture   = transaction.openTable("region");
    auto table       = tIdFuture.get();
    std::string line;
    std::ifstream infile("ch-tables/region.tbl");
    while (std::getline(infile, line)) {
        auto items = tpcc::split(line, '|');
        if (items.size() != 3) {
            LOG_ERROR("region file must contain of 3-tuples!");
            return;
        }
        int16_t intKey = static_cast<int16_t>(std::stoi(items[0]));
        tell::db::key_t key = tell::db::key_t{static_cast<uint64_t>(intKey)};
        transaction.insert(table, key, {{
            {"r_regionkey", intKey},
            {"r_name", crossbow::string(items[1])},
            {"r_comment", crossbow::string(items[2])}
        }});
    }
}

void Populator::populateNations(Transaction &transaction)
{
    auto tIdFuture   = transaction.openTable("nation");
    auto table       = tIdFuture.get();
    std::string line;
    std::ifstream infile("ch-tables/nation.tbl");
    while (std::getline(infile, line)) {
        auto items = tpcc::split(line, '|');
        if (items.size() != 4) {
            LOG_ERROR("nation file must contain of 4-tuples!");
            return;
        }
        int16_t intKey = static_cast<int16_t>(std::stoi(items[0]));
        tell::db::key_t key = tell::db::key_t{static_cast<uint64_t>(intKey)};
        transaction.insert(table, key, {{
            {"n_nationkey", intKey},
            {"n_name", crossbow::string(items[1])},
            {"n_regionkey", static_cast<int16_t>(std::stoi(items[2]))},
            {"n_comment", crossbow::string(items[3])}
        }});
    }
}

void Populator::populateSuppliers(Transaction &transaction)
{
    auto tIdFuture   = transaction.openTable("supplier");
    auto table       = tIdFuture.get();
    std::string line;
    std::ifstream infile("ch-tables/supplier.tbl");
    while (std::getline(infile, line)) {
        auto items = tpcc::split(line, '|');
        if (items.size() != 7) {
            LOG_ERROR("supplier file must contain of 7-tuples!");
            return;
        }
        int16_t intKey = static_cast<int16_t>(std::stoi(items[0]));
        tell::db::key_t key = tell::db::key_t{static_cast<uint64_t>(intKey)};
        std::string acctbal = items[5];
        acctbal.erase(acctbal.find("."), 1);
        transaction.insert(table, key, {{
            {"su_suppkey", intKey},
            {"su_name", crossbow::string(items[1])},
            {"su_address", crossbow::string(items[2])},
            {"su_nationkey", static_cast<int16_t>(std::stoi(items[3]))},
            {"su_phone", crossbow::string(items[4])},
            {"su_acctbal", static_cast<int64_t>(std::stoll(acctbal))},
            {"su_comment", crossbow::string(items[6])}
        }});
    }
}

void Populator::populateStocks(tell::db::Transaction &transaction,
                               int16_t w_id, bool useCH) {
    auto tIdFuture   = transaction.openTable("stock");
    auto table       = tIdFuture.get();
    uint64_t keyBase = uint64_t(w_id);
    keyBase = keyBase << 32;
    for (int32_t s_i_id = 1; s_i_id <= 100000; ++s_i_id) {
        tell::db::key_t key = tell::db::key_t{keyBase | uint64_t(s_i_id)};
        auto s_data = mRandom.astring(26, 50);
        if (mRandom.randomWithin(0, 9) == 0) {
            if (s_data.size() > 42) {
                s_data.resize(42);
            }
            std::uniform_int_distribution<size_t> dist(0, s_data.size());
            auto iter = s_data.begin() + dist(mRandom.randomDevice());
            s_data.insert(iter, mOriginal.begin(), mOriginal.end());
        }

        std::unordered_map<crossbow::string, Field> tuple =
        {{{"s_i_id", s_i_id},
        {"s_w_id", w_id},
        {"s_quantity", int(mRandom.randomWithin(10, 100))},
        {"s_dist_01", mRandom.astring(24, 24)},
        {"s_dist_02", mRandom.astring(24, 24)},
        {"s_dist_03", mRandom.astring(24, 24)},
        {"s_dist_04", mRandom.astring(24, 24)},
        {"s_dist_05", mRandom.astring(24, 24)},
        {"s_dist_06", mRandom.astring(24, 24)},
        {"s_dist_07", mRandom.astring(24, 24)},
        {"s_dist_08", mRandom.astring(24, 24)},
        {"s_dist_09", mRandom.astring(24, 24)},
        {"s_dist_10", mRandom.astring(24, 24)},
        {"s_ytd", int(0)},
        {"s_order_cnt", int16_t(0)},
        {"s_remote_cnt", int16_t(0)},
        {"s_data", std::move(s_data)}}};
        if (useCH)
            tuple.emplace("s_su_suppkey", int16_t(mRandom.randomWithin(0,99)));

        transaction.insert(
          table, key, tuple);
    }
}

void Populator::populateDistricts(tell::db::Transaction &transaction,
                                  Counter &counter, int16_t w_id, bool useCH) {
    auto tIdFuture   = transaction.openTable("district");
    auto table       = tIdFuture.get();
    uint64_t keyBase = w_id;
    keyBase          = keyBase << 8;
    auto n = now();
    for (int16_t i = 1u; i <= 10; ++i) {
        uint64_t key = keyBase | uint64_t(i);
        transaction.insert(table, tell::db::key_t{key},
                           {{{"d_id", i},
                             {"d_w_id", w_id},
                             {"d_name", mRandom.astring(6, 10)},
                             {"d_street_1", mRandom.astring(10, 20)},
                             {"d_street_2", mRandom.astring(10, 20)},
                             {"d_city", mRandom.astring(10, 20)},
                             {"d_state", mRandom.astring(2, 2)},
                             {"d_zip", mRandom.zipCode()},
                             {"d_tax", int(mRandom.randomWithin(0, 2000))},
                             {"d_ytd", int64_t(3000000)},
                             {"d_next_o_id", int(3001)}}});
        populateCustomers(transaction, counter, w_id, i, n, useCH);
        populateOrders(transaction, i, w_id, n);
        populateNewOrders(transaction, w_id, i);
    }
}

void Populator::populateCustomers(tell::db::Transaction &transaction,
                                  Counter &counter, int16_t w_id, int16_t d_id,
                                  int64_t c_since, bool useCH) {
    auto tIdFuture   = transaction.openTable("customer");
    auto table       = tIdFuture.get();
    uint64_t keyBase = uint64_t(w_id) << (5 * 8);
    keyBase |= (uint64_t(d_id) << 4 * 8);
    for (int32_t c_id = 1; c_id <= 3000; ++c_id) {
        crossbow::string c_credit("GC");
        if (mRandom.randomWithin(0, 9) == 0) {
            c_credit = "BC";
        }
        uint64_t key = keyBase | uint64_t(c_id);
        int32_t rNum = c_id - 1;
        if (rNum >= 1000) {
            rNum = mRandom.NURand<int32_t>(255, 0, 999);
        }

        std::unordered_map<crossbow::string, Field> tuple =
            {{{"c_id", c_id},
              {"c_d_id", d_id},
              {"c_w_id", w_id},
              {"c_first", mRandom.astring(8, 16)},
              {"c_middle", crossbow::string("OE")},
              {"c_last", mRandom.cLastName(rNum)},
              {"c_street_1", mRandom.astring(10, 20)},
              {"c_street_2", mRandom.astring(10, 20)},
              {"c_city", mRandom.astring(10, 20)},
              {"c_state", mRandom.astring(2, 2)},
              {"c_zip", mRandom.zipCode()},
              {"c_phone", mRandom.nstring(16, 16)},
              {"c_since", c_since},
              {"c_credit", c_credit},
              {"c_credit_lim", int64_t(5000000)},
              {"c_discount", int(mRandom.randomWithin(0, 50000))},
              {"c_balance", int64_t(-1000)},
              {"c_ytd_payment", int64_t(1000)},
              {"c_payment_cnt", int16_t(1)},
              {"c_delivery_cnt", int16_t(0)},
              {"c_data", mRandom.astring(300, 500)}}};
        if (useCH)
            tuple.emplace("c_n_nationkey", int16_t(mRandom.randomWithin(0,24)));

        transaction.insert(
          table, tell::db::key_t{key}, tuple);
        populateHistory(transaction, counter, c_id, d_id, w_id, c_since);
    }
}

void Populator::populateHistory(tell::db::Transaction &transaction,
                                tell::db::Counter &counter, int32_t c_id,
                                int16_t d_id, int16_t w_id, int64_t n) {
    uint64_t key   = counter.next();
    auto tIdFuture = transaction.openTable("history");
    auto table = tIdFuture.get();
    transaction.insert(table, tell::db::key_t{key},
                       {{{"h_c_id", c_id},
                         {"h_c_d_id", d_id},
                         {"h_c_w_id", w_id},
                         {"h_d_id", d_id},
                         {"h_w_id", w_id},
                         {"h_date", n},
                         {"h_amount", int32_t(1000)},
                         {"h_data", mRandom.astring(12, 24)}}});
}

void Populator::populateOrders(tell::db::Transaction &transaction, int16_t d_id,
                               int16_t w_id, int64_t o_entry_d) {
    auto tIdFuture = transaction.openTable("order");
    auto table = tIdFuture.get();
    std::vector<int32_t> c_ids(3000, 0);
    for (int32_t i = 0; i < 3000; ++i) {
        c_ids[i] = i + 1;
    }
    std::shuffle(c_ids.begin(), c_ids.end(), mRandom.randomDevice());
    uint64_t baseKey = (uint64_t(w_id) << 5 * 8) | (uint64_t(d_id) << 32);
    for (int o_id = 1; o_id <= 3000; ++o_id) {
        auto o_ol_cnt = int16_t(mRandom.randomWithin(5, 15));
        tell::db::key_t key{baseKey | uint64_t(o_id)};
        std::unordered_map<crossbow::string, Field> t{
          {{"o_id", o_id},
           {"o_d_id", d_id},
           {"o_w_id", w_id},
           {"o_c_id", c_ids[o_id - 1]},
           {"o_entry_d", o_entry_d},
           {"o_carrier_id", nullptr},
           {"o_ol_cnt", o_ol_cnt},
           {"o_all_local", int16_t(1)}}};
        if (o_id <= 2100) {
            t["o_carrier_id"] = mRandom.random<int16_t>(1, 10);
        }
        transaction.insert(table, key, t);
        populateOrderLines(transaction, o_id, d_id, w_id, o_ol_cnt, o_entry_d);
    }
}

void Populator::populateOrderLines(tell::db::Transaction &transaction,
                                   int32_t o_id, int16_t d_id, int16_t w_id,
                                   int16_t ol_cnt, int64_t o_entry_d) {
    auto tIdFuture   = transaction.openTable("order-line");
    auto table       = tIdFuture.get();
    uint64_t baseKey = (uint64_t(w_id) << 6 * 8) | (uint64_t(d_id) << 5 * 8) |
                       (uint64_t(o_id) << 8);
    for (int16_t ol_number = 1; ol_number <= ol_cnt; ++ol_number) {
        tell::db::key_t key{baseKey | uint64_t(ol_number)};
        transaction.insert(
          table, key,
          {{{"ol_o_id", o_id},
            {"ol_d_id", d_id},
            {"ol_w_id", w_id},
            {"ol_number", ol_number},
            {"ol_i_id", mRandom.random<int32_t>(1, 100000)},
            {"ol_supply_w_id", w_id},
            {"ol_delivery_d", o_id < 2101 ? Field(o_entry_d) : Field(nullptr)},
            {"ol_quantity", int16_t(5)},
            {"ol_amount", o_id < 2101
                            ? int32_t(0)
                            : mRandom.randomWithin<int32_t>(1, 999999)},
            {"ol_dist_info", mRandom.astring(24, 24)}}});
        // Only for debugging
//        if (o_id <= 10) {
//            std::cout << "ol_o_id: " << o_id
//                        << "ol_d_id: " << d_id
//                        << "ol_id: " << w_id
//                        << "ol_w_id: " << w_id
//                        << "ol_number: " << ol_number
//                        << "ol_supply_w_id: " << w_id
//                        << "ol_delivery_d: " << (o_id < 2101 ? o_entry_d : 0)
//                        << "ol_quantity: " << 5
//                        << "ol_amount: " << o_id
//                        << std::endl;
//        }
    }
}

void Populator::populateNewOrders(tell::db::Transaction &transaction,
                                  int16_t w_id, int16_t d_id) {
    auto tIdFuture   = transaction.openTable("new-order");
    auto table       = tIdFuture.get();
    uint64_t baseKey = (uint64_t(w_id) << 5 * 8) | (uint64_t(d_id) << 4 * 8);
    for (int32_t o_id = 2101; o_id <= 3000; ++o_id) {
        tell::db::key_t key{baseKey | uint64_t(o_id)};
        transaction.insert(
          table, key,
          {{{"no_o_id", o_id}, {"no_d_id", d_id}, {"no_w_id", w_id}}});
    }
}

} // namespace tpcc
