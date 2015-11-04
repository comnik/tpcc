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


Future<Tuple> Transactions::getCustomer(Transaction& tx,
        bool selectByLastName,
        const crossbow::string& c_last,
        int16_t c_w_id,
        int16_t c_d_id,
        table_t customerTable,
        CustomerKey& customerKey) {
    if (selectByLastName) {
        auto iter = tx.lower_bound(customerTable, "c_last_idx",
                std::vector<Field>({
                    Field::create(c_last)
                    , Field::create(c_w_id)
                    , Field::create(c_d_id)
                    , Field::create("")
                    }));
        std::vector<tell::db::key_t> keys;
        for (; !iter.done(); iter.next()) {
            auto k = iter.key();
            if (boost::any_cast<const crossbow::string&>(k[0].value()) != c_last) {
                break;
            }
            keys.push_back(iter.value());
        }
        auto pos = keys.size();
        pos = pos % 2 == 0 ? (pos / 2) : (pos / 2 + 1);
        customerKey = CustomerKey{keys[pos]};
    } else {
        auto c_id = rnd.NURand<int32_t>(1023,1,3000);
        customerKey = CustomerKey{c_w_id, c_d_id, c_id};
    }
    return tx.get(customerTable, customerKey.key());
}

PaymentResult Transactions::payment(tell::db::Transaction& tx, const PaymentIn& in) {
    PaymentResult result;
    try {
        auto cTableF = tx.openTable("customer");
        auto wTableF = tx.openTable("warehouse");
        auto dTableF = tx.openTable("district");
        auto hTableF = tx.openTable("history");
        auto hTable = hTableF.get();
        auto dTable = dTableF.get();
        auto wTable = wTableF.get();
        auto cTable = cTableF.get();
        CustomerKey customerKey(0, 0, 0);
        auto customerF = getCustomer(tx, in.selectByLastName, in.c_last,
               in.c_w_id, in.c_d_id, cTable, customerKey);
        DistrictKey dKey{in.w_id, in.d_id};
        auto districtF = tx.get(dTable, dKey.key());
        tell::db::key_t warehouseKey{uint64_t(in.w_id)};
        auto warehouseF = tx.get(wTable, warehouseKey);
        auto warehouse = warehouseF.get();
        auto nWarehouse = warehouse;
        // update the warehouses ytd
        {
            auto value = boost::any_cast<int64_t>(warehouse.at("w_ytd").value());
            nWarehouse.at("w_ytd") = Field::create(int64_t(value + in.h_amount));
        }
        tx.update(wTable, warehouseKey, warehouse, nWarehouse);
        auto district = districtF.get();
        auto nDistrict = district;
        {
            auto value = boost::any_cast<int64_t>(district.at("d_ytd").value());
            nDistrict.at("d_ytd") = Field::create(int64_t(value + in.h_amount));
        }
        tx.update(dTable, dKey.key(), district, nDistrict);
        auto customer = customerF.get();
        auto nCustomer = customer;
        {
            nCustomer.at("c_balance") += Field::create(int64_t(in.h_amount));
            nCustomer.at("c_ytd_payment") += Field::create(int64_t(in.h_amount));
            nCustomer.at("c_payment_cnt") += Field::create(int16_t(in.h_amount));
            if (boost::any_cast<const crossbow::string&>(customer.at("c_credit").value()) == "BC") {
                crossbow::string histInfo = "(" + crossbow::to_string(customerKey.c_id) +
                    "," + crossbow::to_string(customerKey.d_id) + "," + crossbow::to_string(customerKey.w_id) +
                    "," + crossbow::to_string(in.d_id) + "," + crossbow::to_string(in.w_id) +
                    "," + crossbow::to_string(in.h_amount);
                auto c_data = boost::any_cast<crossbow::string>(nCustomer.at("c_data").value());
                c_data.insert(0, histInfo);
                if (c_data.size() > 500) {
                    c_data.resize(500);
                }
            }
        }
        tx.update(cTable, customerKey.key(), customer, nCustomer);
        // insert into history
        crossbow::string h_data = boost::any_cast<const crossbow::string&>(warehouse.at("w_name").value())
            + boost::any_cast<const crossbow::string&>(district.at("d_name").value());
        tell::db::key_t historyKey{
            uint64_t(rnd.randomWithin<uint32_t>(0, std::numeric_limits<uint32_t>::max())) << 8*sizeof(uint32_t) |
            uint64_t(rnd.randomWithin<uint32_t>(0, std::numeric_limits<uint32_t>::max()))
        };
        while (true) {
            auto h = tx.get(hTable, historyKey);
            try {
                h.get();
            } catch (...) {
                break;
            }
            historyKey = tell::db::key_t{
                uint64_t(rnd.randomWithin<uint32_t>(0, std::numeric_limits<uint32_t>::max())) << 8*sizeof(uint32_t) |
                    uint64_t(rnd.randomWithin<uint32_t>(0, std::numeric_limits<uint32_t>::max()))
            };
        }
        tx.insert(hTable, historyKey,
                {{
                {"h_c_id", customerKey.c_id},
                {"h_c_d_id", customerKey.d_id},
                {"h_c_w_id", customerKey.w_id},
                {"h_d_id", in.d_id},
                {"h_w_id", in.w_id},
                {"h_date", now()},
                {"h_amount", in.h_amount},
                {"h_data", h_data}
                }});
        tx.commit();
        result.success = true;
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }
    return result;
}

} // namespace tpcc

