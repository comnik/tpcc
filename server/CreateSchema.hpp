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
#pragma once
#include <telldb/Types.hpp>
#include <limits>

#include <boost/functional/hash.hpp>

namespace tell {
namespace db {

class Transaction;

} // namespace db
} // namespace tell

namespace tpcc {

void createSchema(tell::db::Transaction& transaction, bool useCH);

struct WarehouseKey {
    int16_t w_id;

    WarehouseKey(tell::db::key_t k)
        : w_id(int16_t(k.value))
    {}

    WarehouseKey(int64_t w_id) : w_id(w_id) {}

    tell::db::key_t key() const {
        return tell::db::key_t{uint64_t(w_id)};
    }
};

struct DistrictKey {
    int16_t w_id;
    int16_t d_id;

    DistrictKey(int16_t w_id, int16_t d_id)
        : w_id(w_id)
        , d_id(d_id)
    {}

    DistrictKey(tell::db::key_t key)
        : w_id( key.value >> 8)
        , d_id(0xff & key.value)
    {}

    tell::db::key_t key() const {
        return tell::db::key_t{(uint64_t(w_id) << 8) | d_id};
    }
};

struct CustomerKey {
    int16_t w_id;
    int16_t d_id;
    int32_t c_id;

    CustomerKey(int16_t w_id, int16_t d_id, int32_t c_id)
        : w_id(w_id)
        , d_id(d_id)
        , c_id(c_id)
    {}

    CustomerKey(tell::db::key_t key)
        : w_id(key.value >> 5*8)
        , d_id((key.value >> 4*8) & 0xff)
        , c_id(key.value & 0xffffffff)
    {}

    tell::db::key_t key() const {
        return tell::db::key_t{(uint64_t(w_id) << 5*8) | (uint64_t(d_id) << 4*8) | uint64_t(c_id)};
    }
};

struct NewOrderKey {
    int16_t w_id;
    int16_t d_id;
    int32_t o_id;

    NewOrderKey(int16_t w_id, int16_t d_id, int32_t o_id)
        : w_id(w_id)
        , d_id(d_id)
        , o_id(o_id)
    {}

    NewOrderKey(tell::db::key_t k)
        : w_id(k.value >> 5*8)
        , d_id((k.value >> 4*8) & 0xff)
        , o_id(k.value & std::numeric_limits<uint32_t>::max())
    {}

    tell::db::key_t key() const {
        return tell::db::key_t{(uint64_t(w_id) << 5*8) | (uint64_t(d_id) << 4*8) | uint64_t(o_id)};
    }
};

using OrderKey = NewOrderKey;

struct OrderlineKey {
    int16_t w_id;
    int16_t d_id;
    int32_t o_id;
    int16_t ol_number;

    OrderlineKey(int16_t w_id, int16_t d_id, int32_t o_id, int16_t ol_number)
        : w_id(w_id)
        , d_id(d_id)
        , o_id(o_id)
        , ol_number(ol_number)
    {}

    OrderlineKey(tell::db::key_t k)
        : w_id(k.value >> 6*8)
        , d_id((k.value >> 5*8) & 0xff)
        , o_id((k.value >> 8) & std::numeric_limits<uint32_t>::max())
        , ol_number(k.value & 0xff)
    {}

    tell::db::key_t key() const {
        return tell::db::key_t{(uint64_t(w_id) << 6*8) | (uint64_t(d_id) << 5*8) | (uint64_t(o_id) << 8) | uint64_t(ol_number)};
    }
};

struct ItemKey {
    int32_t i_id;

    ItemKey(int32_t i_id)
        : i_id(i_id)
    {}

    ItemKey(tell::db::key_t k)
        : i_id(k.value)
    {}

    tell::db::key_t key() const {
        return tell::db::key_t{uint64_t(i_id)};
    }
};

inline bool operator ==(const ItemKey& lhs, const ItemKey& rhs) {
    return lhs.i_id == rhs.i_id;
}

inline bool operator !=(const ItemKey& lhs, const ItemKey& rhs) {
    return !(lhs == rhs);
}

struct StockKey {
    int16_t w_id;
    int32_t i_id;

    StockKey(int16_t w_id, int32_t i_id)
        : w_id(w_id)
        , i_id(i_id)
    {}

    StockKey(tell::db::key_t k)
        : w_id(k.value >> 4*8)
        , i_id(k.value & std::numeric_limits<uint32_t>::max())
    {}

    tell::db::key_t key() const {
        return tell::db::key_t{(uint64_t(w_id) << 4*8) | uint64_t(i_id)};
    }
};

inline bool operator ==(const StockKey& lhs, const StockKey& rhs) {
    return (lhs.w_id == rhs.w_id && lhs.i_id == rhs.i_id);
}

inline bool operator !=(const StockKey& lhs, const StockKey& rhs) {
    return !(lhs == rhs);
}

} // namespace tpcc

namespace std {

template <>
struct hash<tpcc::StockKey> {
    size_t operator ()(const tpcc::StockKey& value) const {
        size_t seed = 0;
        boost::hash_combine(seed, std::hash<decltype(value.w_id)>()(value.w_id));
        boost::hash_combine(seed, std::hash<decltype(value.i_id)>()(value.i_id));
        return seed;
    }
};

template <>
struct hash<tpcc::ItemKey> {
    size_t operator ()(const tpcc::ItemKey& value) const {
        return std::hash<decltype(value.i_id)>()(value.i_id);
    }
};

} // namespace std
