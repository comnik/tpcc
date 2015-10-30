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
#include "Populate.hpp"
#include <telldb/Transaction.hpp>
#include <chrono>
#include <algorithm>

using namespace tell::db;

namespace tpcc {

void Populator::populateItems(tell::db::Transaction& transaction) {
    auto tIdFuture = transaction.openTable("item");
    auto tId = tIdFuture.get();
    for (int i = 1; i <= 100000; ++i) {
        transaction.insert(tId, tell::db::key_t{uint64_t(i)},
                std::make_tuple(i, // i_id
                    int(mRandom.randomWithin(1, 10000)), // i_im_id
                    mRandom.astring(14, 24), // i_name
                    int(mRandom.randomWithin(100, 10000)), // i_price
                    mRandom.astring(26, 50) // i_data
                    ));
    }
}

void Populator::populateWarehouse(tell::db::Transaction& transaction, Counter& counter, int16_t w_id) {
    auto tIdFuture = transaction.openTable("warehouse");
    auto table = tIdFuture.get();
    tell::db::key_t key{uint64_t(w_id)};
    transaction.insert(table, key, std::make_tuple(
                w_id,
                mRandom.astring(6, 10), // w_name
                mRandom.astring(10, 20), // w_street_1
                mRandom.astring(10, 20), // w_street_2
                mRandom.astring(10, 20), // w_city
                mRandom.astring(2, 2), // w_state
                mRandom.zipCode(), // w_zip
                int(mRandom.randomWithin(0, 2000)), // w_tax
                int64_t(30000000)
                ));
    populateStocks(transaction, w_id);
    populateDistricts(transaction, counter, w_id);
}

void Populator::populateStocks(tell::db::Transaction& transaction, int16_t w_id) {
    auto tIdFuture = transaction.openTable("stock");
    auto table = tIdFuture.get();
    uint64_t keyBase = uint64_t(w_id);
    keyBase = keyBase << 32;
    for (int s_i_id = 1; s_i_id <= 100000; ++s_i_id) {
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
        transaction.insert(table, key, std::make_tuple(
                    s_i_id,
                    w_id, // s_w_id
                    int(mRandom.randomWithin(10, 100)), // s_quantity
                    mRandom.astring(24, 24), // s_dist_01
                    mRandom.astring(24, 24), // s_dist_02
                    mRandom.astring(24, 24), // s_dist_03
                    mRandom.astring(24, 24), // s_dist_04
                    mRandom.astring(24, 24), // s_dist_05
                    mRandom.astring(24, 24), // s_dist_06
                    mRandom.astring(24, 24), // s_dist_07
                    mRandom.astring(24, 24), // s_dist_08
                    mRandom.astring(24, 24), // s_dist_09
                    mRandom.astring(24, 24), // s_dist_10
                    int(0), //s_ytd
                    int16_t(0), // s_order_cnt
                    int16_t(0), // s_remote_cnt
                    std::move(s_data)
                    ));
    }
}

void Populator::populateDistricts(tell::db::Transaction& transaction, Counter& counter, int16_t w_id) {
    auto tIdFuture = transaction.openTable("district");
    auto table = tIdFuture.get();
    uint64_t keyBase = w_id;
    keyBase = keyBase << 8;
    auto n = now();
    for (int16_t i = 1u; i <= 10; ++i) {
        uint64_t key = keyBase | uint64_t(i);
        transaction.insert(table, tell::db::key_t{key}, std::make_tuple(
                    i, // d_i_d
                    w_id, // d_w_id
                    mRandom.astring(6, 10), // d_name
                    mRandom.astring(10, 20), // d_street_1
                    mRandom.astring(10, 20), // d_street_2
                    mRandom.astring(10, 20), // d_city
                    mRandom.astring(2, 2), // d_state
                    mRandom.zipCode(), // d_zip
                    int(mRandom.randomWithin(0, 2000)), // d_tax
                    int64_t(3000000), // d_ytd
                    int(3001) // d_next_o_id
                    ));
        populateCustomers(transaction, counter, w_id, i, n);
        populateOrders(transaction, i, w_id, n);
        populateNewOrders(transaction, w_id, i);
    }
}

void Populator::populateCustomers(tell::db::Transaction& transaction, Counter& counter, int16_t w_id, int16_t d_id, int64_t c_since) {
    auto tIdFuture = transaction.openTable("customer");
    auto table = tIdFuture.get();
    uint64_t keyBase = uint64_t(w_id) << (3*8);
    keyBase |= (uint64_t(d_id) << 2*8);
    for (int16_t c_id = 1; c_id <= 3000; ++c_id) {
        crossbow::string c_credit("GC");
        if (mRandom.randomWithin(0, 9) == 0) {
            c_credit = "BC";
        }
        uint64_t key = keyBase | uint64_t(c_id);
        int rNum = c_id;
        if (rNum >= 1000) {
            rNum = int16_t(mRandom.NURand<int16_t>(255, 0, 999));
        }
        transaction.insert(table, tell::db::key_t{key}, std::make_tuple(
                    c_id
                    , d_id
                    , w_id
                    , mRandom.astring(8, 16) // c_first
                    , crossbow::string("OE") // c_middle
                    , mRandom.cLastName(rNum) // c_last
                    , mRandom.astring(10, 20) // c_street_1
                    , mRandom.astring(10, 20) // c_street_2
                    , mRandom.astring(10, 20) // c_city
                    , mRandom.astring(2, 2) // c_state
                    , mRandom.zipCode() // c_zip
                    , mRandom.nstring(16, 16) // c_phone
                    , c_since
                    , c_credit
                    , int64_t(5000000) // c_credit_lim
                    , int(mRandom.randomWithin(0, 50000)) // c_discount
                    , int64_t(-1000) // c_balance
                    , int64_t(1000) // c_ytd_payment
                    , int16_t(1) // c_payment_cnt
                    , int16_t(0) // c_delivery_cnt
                    , mRandom.astring(300, 500) // c_data
                    ));
        populateHistory(transaction, counter, c_id, d_id, w_id, c_since);
    }
}

void Populator::populateHistory(tell::db::Transaction& transaction, tell::db::Counter& counter, int16_t c_id, int16_t d_id, int16_t w_id, int64_t n) {
    uint64_t key = counter.next();
    auto tIdFuture = transaction.openTable("history");
    auto table = tIdFuture.get();
    transaction.insert(table, tell::db::key_t{key}, std::make_tuple(
                c_id, // h_c_id
                d_id, // h_c_d_id
                w_id, // h_c_w_id
                n, // h_date
                int(100), // h_amount
                mRandom.astring(12, 24) // h_data
                ));
}

void Populator::populateOrders(tell::db::Transaction& transaction, int16_t d_id, int16_t w_id, int64_t o_entry_d) {
    auto tIdFuture = transaction.openTable("order");
    auto table = tIdFuture.get();
    std::vector<int32_t> c_ids(3000, 0);
    for (int32_t i = 0; i < 3000; ++i) {
        c_ids[i] = i;
    }
    std::shuffle(c_ids.begin(), c_ids.end(), mRandom.randomDevice());
    uint64_t baseKey = (uint64_t(w_id) << 5*8) | (uint64_t(d_id) << 32);
    for (int o_id = 1; o_id <= 3000; ++o_id) {
        auto o_ol_cnt = int16_t(mRandom.randomWithin(5, 15));
        tell::db::key_t key{baseKey | uint64_t(o_id)};
        if (o_id < 2101) {
            transaction.insert(table, key, std::make_tuple(
                        o_id
                        , c_ids[o_id - 1]
                        , w_id
                        , o_entry_d
                        , int16_t(mRandom.randomWithin(1, 10)) // o_carrier_id
                        , o_ol_cnt
                        , int16_t(1)
                        ));
        } else {
            transaction.insert(table, key, std::make_tuple(
                        o_id
                        , c_ids[o_id - 1]
                        , w_id
                        , o_entry_d
                        , nullptr // o_carrier_id
                        , o_ol_cnt
                        , int16_t(1)
                        ));
        }
        populateOrderLines(transaction, o_id, d_id, w_id, o_ol_cnt, o_entry_d);
    }
}

void Populator::populateOrderLines(tell::db::Transaction& transaction,
        int32_t o_id,
        int16_t d_id,
        int16_t w_id,
        int16_t ol_cnt,
        int64_t o_entry_d) {
    auto tIdFuture = transaction.openTable("order-line");
    auto table = tIdFuture.get();
    uint64_t baseKey = (uint64_t(w_id) << 6*8) | (uint64_t(d_id) << 5*8) | (uint64_t(o_id) << 8);
    for (int16_t ol_number = 1; ol_number <= ol_cnt; ++ol_number) {
        tell::db::key_t key{baseKey | uint64_t(ol_number)};
        transaction.insert(table, key, std::make_tuple(
                    o_id
                    , d_id
                    , w_id
                    , ol_number
                    , int32_t(mRandom.randomWithin(1, 100000)) // ol_i_id
                    , w_id // old_supply_w_id
                    , o_id < 2101 ? o_entry_d : int64_t(mRandom.randomWithin(1, 999999)) // ol_delivery_d
                    , mRandom.astring(24, 24) // ol_dist_info
                    ));
    }
}

void Populator::populateNewOrders(tell::db::Transaction& transaction, int16_t w_id, int16_t d_id) {
    auto tIdFuture = transaction.openTable("new-order");
    auto table = tIdFuture.get();
    uint64_t baseKey = (uint64_t(w_id) << 5*8) | (uint64_t(d_id) << 4*8);
    for (int16_t o_id = 2101; o_id <= 3000; ++o_id) {
        tell::db::key_t key{baseKey | uint64_t(o_id)};
        transaction.insert(table, key, std::make_tuple(
                    o_id
                    , d_id
                    , w_id
                    ));
    }
}

} // namespace tpcc

