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

namespace tpcc {

template<typename Gen, typename Int>
Int random(Gen& g, Int lower, Int upper)
{
    std::uniform_int_distribution<Int> dist(lower, upper);
    return dist(g);
}

template<typename Gen, typename Int>
Int NURand(Gen& g, Int A, Int x, Int y)
{
    //constexpr int A = y < 1000 ? 255: (y <= 3000 ? 1023 : 8191);
    constexpr int C = 0;
    return (((random(g,0,A) | random(g,x,y)) + C) % (y - x + 1)) + x;
}

void Populator::populateItems(tell::db::Transaction& transaction) {
    auto tIdFuture = transaction.openTable("item");
    auto tId = tIdFuture.get();
    for (int i = 1; i <= 100000; ++i) {
        transaction.insert(tId, tell::db::key_t{uint64_t(i)},
                std::make_tuple(i, // i_id
                    int(randomWithin(1, 10000)), // i_im_id
                    astring(14, 24), // i_name
                    int(randomWithin(100, 10000)), // i_price
                    astring(26, 50) // i_data
                    ));
    }
}

void Populator::populateWarehouse(tell::db::Transaction& transaction, int16_t w_id) {
    auto tIdFuture = transaction.openTable("warehouse");
    auto table = tIdFuture.get();
    tell::db::key_t key{uint64_t(w_id)};
    transaction.insert(table, key, std::make_tuple(
                w_id,
                astring(6, 10), // w_name
                astring(10, 20), // w_street_1
                astring(10, 20), // w_street_2
                astring(10, 20), // w_city
                astring(2, 2), // w_state
                zipCode(), // w_zip
                int(randomWithin(0, 2000)), // w_tax
                int64_t(30000000)
                ));
    populateStocks(transaction, w_id);
    populateDistricts(transaction, w_id);
}

void Populator::populateStocks(tell::db::Transaction& transaction, int16_t w_id) {
    auto tIdFuture = transaction.openTable("stock");
    auto table = tIdFuture.get();
    uint64_t keyBase = uint64_t(w_id);
    keyBase = keyBase << 32;
    for (int s_i_id = 1; s_i_id <= 100000; ++s_i_id) {
        tell::db::key_t key = tell::db::key_t{keyBase | uint64_t(s_i_id)};
        auto s_data = astring(26, 50);
        if (randomWithin(0, 9) == 0) {
            if (s_data.size() > 42) {
                s_data.resize(42);
            }
            std::uniform_int_distribution<size_t> dist(0, s_data.size());
            auto iter = s_data.begin() + dist(mRandomDevice);
            s_data.insert(iter, mOriginal.begin(), mOriginal.end());
        }
        transaction.insert(table, key, std::make_tuple(
                    s_i_id,
                    w_id, // s_w_id
                    int(randomWithin(10, 100)), // s_quantity
                    astring(24, 24), // s_dist_01
                    astring(24, 24), // s_dist_02
                    astring(24, 24), // s_dist_03
                    astring(24, 24), // s_dist_04
                    astring(24, 24), // s_dist_05
                    astring(24, 24), // s_dist_06
                    astring(24, 24), // s_dist_07
                    astring(24, 24), // s_dist_08
                    astring(24, 24), // s_dist_09
                    astring(24, 24), // s_dist_10
                    int(0), //s_ytd
                    int16_t(0), // s_order_cnt
                    int16_t(0), // s_remote_cnt
                    std::move(s_data)
                    ));
    }
}

void Populator::populateDistricts(tell::db::Transaction& transaction, int16_t w_id) {
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
                    astring(6, 10), // d_name
                    astring(10, 20), // d_street_1
                    astring(10, 20), // d_street_2
                    astring(10, 20), // d_city
                    astring(2, 2), // d_state
                    zipCode(), // d_zip
                    int(randomWithin(0, 2000)), // d_tax
                    int64_t(3000000), // d_ytd
                    int(3001) // d_next_o_id
                    ));
        populateCustomers(transaction, w_id, i, n);
        populateOrders(transaction, i, w_id, n);
        populateNewOrders(transaction, w_id, i);
    }
}

void Populator::populateCustomers(tell::db::Transaction& transaction, int16_t w_id, int16_t d_id, int64_t c_since) {
    auto tIdFuture = transaction.openTable("customer");
    auto table = tIdFuture.get();
    uint64_t keyBase = uint64_t(w_id) << (3*8);
    keyBase |= d_id;
    keyBase = keyBase << 8;
    for (int16_t c_id = 1; c_id <= 3000; ++c_id) {
        crossbow::string c_credit("GC");
        if (randomWithin(0, 9) == 0) {
            c_credit = "BC";
        }
        uint64_t key = keyBase | uint64_t(c_id);
        int rNum = c_id;
        if (rNum >= 1000) {
            rNum = int16_t(NURand(mRandomDevice, 255, 0, 999));
        }
        transaction.insert(table, tell::db::key_t{key}, std::make_tuple(
                    c_id
                    , d_id
                    , w_id
                    , astring(8, 16) // c_first
                    , crossbow::string("OE") // c_middle
                    , cLastName(rNum) // c_last
                    , astring(10, 20) // c_street_1
                    , astring(10, 20) // c_street_2
                    , astring(10, 20) // c_city
                    , astring(2, 2) // c_state
                    , zipCode() // c_zip
                    , nstring(16, 16) // c_phone
                    , c_since
                    , c_credit
                    , int64_t(5000000) // c_credit_lim
                    , int(randomWithin(0, 50000)) // c_discount
                    , int64_t(-1000) // c_balance
                    , int64_t(1000) // c_ytd_payment
                    , int16_t(1) // c_payment_cnt
                    , int16_t(0) // c_delivery_cnt
                    , astring(300, 500) // c_data
                    ));
        populateHistory(transaction, c_id, d_id, w_id, c_since);
    }
}

void Populator::populateHistory(tell::db::Transaction& transaction, int16_t c_id, int16_t d_id, int16_t w_id, int64_t n) {
    tell::db::key_t key{(uint64_t(c_id) << 3*16) | (uint64_t(d_id) << 2*16) | (uint64_t(w_id) << 16) | uint64_t(0)};
    auto tIdFuture = transaction.openTable("history");
    auto table = tIdFuture.get();
    transaction.insert(table, key, std::make_tuple(
                c_id, // h_c_id
                d_id, // h_c_d_id
                w_id, // h_c_w_id
                n, // h_date
                int(100), // h_amount
                astring(12, 24) // h_data
                ));
}

void Populator::populateOrders(tell::db::Transaction& transaction, int16_t d_id, int16_t w_id, int64_t o_entry_d) {
    auto tIdFuture = transaction.openTable("order");
    auto table = tIdFuture.get();
    std::vector<int32_t> c_ids(3000, 0);
    for (int32_t i = 0; i < 3000; ++i) {
        c_ids[i] = i;
    }
    std::shuffle(c_ids.begin(), c_ids.end(), mRandomDevice);
    uint64_t baseKey = (uint64_t(w_id) << 5*8) | (uint64_t(d_id) << 32);
    for (int o_id = 1; o_id <= 3000; ++o_id) {
        auto o_ol_cnt = int16_t(randomWithin(5, 15));
        tell::db::key_t key{baseKey | uint64_t(o_id)};
        if (o_id < 2101) {
            transaction.insert(table, key, std::make_tuple(
                        o_id
                        , c_ids[o_id - 1]
                        , w_id
                        , o_entry_d
                        , int16_t(randomWithin(1, 10)) // o_carrier_id
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
    for (int16_t ol_number = 1; ol_number <= ol_cnt; ++ol_cnt) {
        tell::db::key_t key{baseKey | uint64_t(ol_number)};
        transaction.insert(table, key, std::make_tuple(
                    o_id
                    , d_id
                    , w_id
                    , ol_number
                    , int32_t(randomWithin(1, 100000)) // ol_i_id
                    , w_id // old_supply_w_id
                    , o_id < 2101 ? o_entry_d : int64_t(randomWithin(1, 999999)) // ol_delivery_d
                    , astring(24, 24) // ol_dist_info
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

crossbow::string Populator::astring(int x, int y) {
    std::uniform_int_distribution<int> lenDist(x, y);
    std::uniform_int_distribution<int> charDist(0, 255);
    auto length = lenDist(mRandomDevice);
    crossbow::string result;
    result.reserve(length);
    for (decltype(length) i = 0; i < length; ++i) {
        int charPos = charDist(mRandomDevice);
        if (charPos < 95) {
            result.push_back(char(0x21 + charPos)); // a printable ASCII/UTF-8 character
            continue;
        }
        constexpr uint16_t lowest6 = 0x3f;
        uint16_t unicodeValue = 0xc0 + (charPos - 95); // a printable unicode character
        // convert this to UTF-8
        uint8_t utf8[2] = {0xc0, 0x80}; // The UTF-8 base for 2-byte Characters
        // for the first char, we have to take the lowest 6 bit
        utf8[1] |= uint8_t(unicodeValue & lowest6);
        utf8[0] |= uint8_t(unicodeValue >> 6); // get the remaining bits
        assert((utf8[0] >> 5) == uint8_t(0x06)); // higher order byte starts with 110
        assert((utf8[1] >> 6) == uint8_t(0x2)); // lower order byte starts with 10
        result.push_back(*reinterpret_cast<char*>(utf8));
        result.push_back(*reinterpret_cast<char*>(utf8 + 1));
    }
    return result;
}

crossbow::string Populator::cLastName(int rNum) {
    crossbow::string res;
    res.reserve(15);
    std::array<crossbow::string, 10> subStrings{{"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"}};
    for (int i = 0; i < 3; ++i) {
        res.append(subStrings[rNum % 10]);
        rNum /= 10;
    }
    return res;
}

namespace {
uint32_t powerOf(uint32_t a, uint32_t x) {
    if (x == 0) return 1;
    if (x % 2 == 0) {
        auto tmp = powerOf(a, x / 2);
        return tmp*tmp;
    }
    return a*powerOf(a, x-1);
}

}

crossbow::string Populator::nstring(unsigned x, unsigned y) {
    std::uniform_int_distribution<uint32_t> lenDist(x, y);
    auto len = lenDist(mRandomDevice);
    std::uniform_int_distribution<uint32_t> dist(0, powerOf(10, len) - 1);
    auto resNum = dist(mRandomDevice);
    crossbow::string res;
    res.resize(len);
    for (decltype(len) i = 0; i < len; ++i) {
        switch (resNum % 10) {
        case 0:
            res[len - i - 1] = '0';
            break;
        case 1:
            res[len - i - 1] = '1';
            break;
        case 2:
            res[len - i - 1] = '2';
            break;
        case 3:
            res[len - i - 1] = '3';
            break;
        case 4:
            res[len - i - 1] = '4';
            break;
        case 5:
            res[len - i - 1] = '5';
            break;
        case 6:
            res[len - i - 1] = '6';
            break;
        case 7:
            res[len - i - 1] = '7';
            break;
        case 8:
            res[len - i - 1] = '8';
            break;
        case 9:
            res[len - i - 1] = '9';
            break;
        }
        resNum /= 10;
    }
    return res;
}

crossbow::string Populator::zipCode() {
    crossbow::string res;
    res.reserve(9);
    res.append(nstring(4, 4));
    res.append("11111");
    return res;
}

int64_t Populator::now() {
    auto now = std::chrono::system_clock::now();
    return now.time_since_epoch().count();
}

} // namespace tpcc

