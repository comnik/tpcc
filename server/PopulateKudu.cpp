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
#include "PopulateKudu.hpp"
#include <chrono>
#include <algorithm>
#include "kudu.hpp"

namespace tpcc {

using namespace kudu::client;

void Populator::populateDimTables(KuduSession &session, bool useCH)
{
    populateItems(session);
    if (useCH) {
        populateRegions(session);
        populateNations(session);
        populateSuppliers(session);
    }
}

void insert(KuduSession& session, const std::string& tableName) {
}

void Populator::populateWarehouse(KuduSession &session,
                                  int16_t w_id, bool useCH) {
    std::tr1::shared_ptr<KuduTable> table;
    assertOk(session.client()->OpenTable("warehouse", &table));
    auto ins = table->NewInsert();
    assertOk(ins->mutable_row()->SetInt16("w_id", w_id));
    assertOk(ins->mutable_row()->SetString("w_name", mRandom.astring(6, 10).c_str()));
    assertOk(ins->mutable_row()->SetString("w_street_1", mRandom.astring(10, 20).c_str()));
    assertOk(ins->mutable_row()->SetString("w_street_2", mRandom.astring(10, 20).c_str()));
    assertOk(ins->mutable_row()->SetString("w_city", mRandom.astring(10, 20).c_str()));
    assertOk(ins->mutable_row()->SetString("w_state", mRandom.astring(10, 20).c_str()));
    assertOk(ins->mutable_row()->SetString("w_zip", mRandom.astring(2, 2).c_str()));
    assertOk(ins->mutable_row()->SetInt32("w_tax", mRandom.random<int32_t>(0, 2000)));
    assertOk(ins->mutable_row()->SetInt64("w_ytd", int64_t(30000000)));
    assertOk(session.Apply(ins));
    populateStocks(session, w_id, useCH);
    populateDistricts(session, w_id, useCH);
    assertOk(session.Flush());
}

void Populator::populateItems(KuduSession &session) {
    std::tr1::shared_ptr<KuduTable> table;
    assertOk(session.client()->OpenTable("item", &table));
    for (int32_t i = 1; i <= 100000; ++i) {
        auto ins = table->NewInsert();
        assertOk(ins->mutable_row()->SetInt32("i_id", i));
        assertOk(ins->mutable_row()->SetInt32("i_im_id", mRandom.randomWithin<int32_t>(1, 10000)));
        assertOk(ins->mutable_row()->SetString("i_name", mRandom.astring(14, 24).c_str()));
        assertOk(ins->mutable_row()->SetInt32("i_price", mRandom.randomWithin<int32_t>(100, 10000)));
        assertOk(ins->mutable_row()->SetString("i_data", mRandom.astring(26, 50).c_str()));
        assertOk(session.Apply(ins));
        if (i % 1000 == 0) assertOk(session.Flush());
    }
    assertOk(session.Flush());
}

void Populator::populateRegions(KuduSession &session)
{
    std::tr1::shared_ptr<KuduTable> table;
    assertOk(session.client()->OpenTable("region", &table));
    std::string line;
    std::ifstream infile("ch-tables/region.tbl");
    while (std::getline(infile, line)) {
        auto items = tpcc::split(line, '|');
        if (items.size() != 3) {
            LOG_ERROR("region file must contain of 3-tuples!");
            return;
        }
        auto ins = table->NewInsert();
        auto row = ins->mutable_row();
        int16_t intKey = static_cast<int16_t>(std::stoi(items[0]));
        assertOk(row->SetInt16("r_regionkey", intKey));
        assertOk(row->SetString("r_name", items[1]));
        assertOk(row->SetString("r_comment", items[2]));
        assertOk(session.Apply(ins));
    }
    assertOk(session.Flush());
}

void Populator::populateNations(KuduSession &session)
{
    std::tr1::shared_ptr<KuduTable> table;
    assertOk(session.client()->OpenTable("nation", &table));
    std::string line;
    std::ifstream infile("ch-tables/nation.tbl");
    while (std::getline(infile, line)) {
        auto items = tpcc::split(line, '|');
        if (items.size() != 4) {
            LOG_ERROR("nation file must contain of 4-tuples!");
            return;
        }
        auto ins = table->NewInsert();
        auto row = ins->mutable_row();
        int16_t intKey = static_cast<int16_t>(std::stoi(items[0]));
        assertOk(row->SetInt16("n_nationkey", intKey));
        assertOk(row->SetString("n_name", items[1]));
        assertOk(row->SetInt16("n_regionkey", static_cast<int16_t>(std::stoi(items[2]))));
        assertOk(row->SetString("n_comment", items[3]));
        assertOk(session.Apply(ins));
    }
    assertOk(session.Flush());
}

void Populator::populateSuppliers(KuduSession &session)
{
    std::tr1::shared_ptr<KuduTable> table;
    assertOk(session.client()->OpenTable("supplier", &table));
    std::string line;
    std::ifstream infile("ch-tables/supplier.tbl");
    while (std::getline(infile, line)) {
        auto items = tpcc::split(line, '|');
        if (items.size() != 7) {
            LOG_ERROR("supplier file must contain of 7-tuples!");
            return;
        }
        int16_t intKey = static_cast<int16_t>(std::stoi(items[0]));
        std::string acctbal = items[5];
        acctbal.erase(acctbal.find("."), 1);
        auto ins = table->NewInsert();
        auto row = ins->mutable_row();
        assertOk(row->SetInt16("su_suppkey", intKey));
        assertOk(row->SetString("su_name", items[1]));
        assertOk(row->SetString("su_address", items[2]));
        assertOk(row->SetInt16("su_nationkey", static_cast<int16_t>(std::stoi(items[3]))));
        assertOk(row->SetString("su_phone", items[4]));
        assertOk(row->SetInt16("su_acctbal", static_cast<int64_t>(std::stoll(acctbal))));
        assertOk(row->SetString("su_comment", items[6]));
        assertOk(session.Apply(ins));
    }
    assertOk(session.Flush());
}

void Populator::populateStocks(KuduSession &session,
                               int16_t w_id, bool useCH) {
    std::tr1::shared_ptr<KuduTable> table;
    assertOk(session.client()->OpenTable("stock", &table));
    for (int32_t s_i_id = 1; s_i_id <= 100000; ++s_i_id) {
        auto s_data = mRandom.astring(26, 50);
        if (mRandom.randomWithin(0, 9) == 0) {
            if (s_data.size() > 42) {
                s_data.resize(42);
            }
            std::uniform_int_distribution<size_t> dist(0, s_data.size());
            auto iter = s_data.begin() + dist(mRandom.randomDevice());
            s_data.insert(iter, mOriginal.begin(), mOriginal.end());
        }

        auto ins = table->NewInsert();
        auto row = ins->mutable_row();
        assertOk(row->SetInt32("s_i_id", s_i_id));
        assertOk(row->SetInt16("s_w_id", w_id));
        assertOk(row->SetInt32("s_quantity", int(mRandom.randomWithin(10, 100))));
        assertOk(row->SetString("s_dist_01", mRandom.astring(24, 24).c_str()));
        assertOk(row->SetString("s_dist_02", mRandom.astring(24, 24).c_str()));
        assertOk(row->SetString("s_dist_03", mRandom.astring(24, 24).c_str()));
        assertOk(row->SetString("s_dist_04", mRandom.astring(24, 24).c_str()));
        assertOk(row->SetString("s_dist_05", mRandom.astring(24, 24).c_str()));
        assertOk(row->SetString("s_dist_06", mRandom.astring(24, 24).c_str()));
        assertOk(row->SetString("s_dist_07", mRandom.astring(24, 24).c_str()));
        assertOk(row->SetString("s_dist_08", mRandom.astring(24, 24).c_str()));
        assertOk(row->SetString("s_dist_09", mRandom.astring(24, 24).c_str()));
        assertOk(row->SetString("s_dist_10", mRandom.astring(24, 24).c_str()));
        assertOk(row->SetInt32("s_ytd", int(0)));
        assertOk(row->SetInt16("s_order_cnt", int16_t(0)));
        assertOk(row->SetInt16("s_remote_cnt", int16_t(0)));
        assertOk(row->SetString("s_data", s_data.c_str()));
        if (useCH)
            assertOk(row->SetInt16("s_su_suppkey", mRandom.randomWithin<int16_t>(1, 10000)));
        assertOk(session.Apply(ins));
        if (s_i_id % 1000 == 0) assertOk(session.Flush());
    }
    assertOk(session.Flush());
}

void Populator::populateDistricts(KuduSession &session,
                                  int16_t w_id, bool useCH) {
    std::tr1::shared_ptr<KuduTable> table;
    assertOk(session.client()->OpenTable("district", &table));
    auto n = now();
    for (int16_t i = 1u; i <= 10; ++i) {
        auto ins = table->NewInsert();
        auto row = ins->mutable_row();
        assertOk(row->SetInt16("d_id", i));
        assertOk(row->SetInt16("d_w_id", w_id));
        assertOk(row->SetString("d_name", mRandom.astring(6, 10).c_str()));
        assertOk(row->SetString("d_street_1", mRandom.astring(10, 20).c_str()));
        assertOk(row->SetString("d_street_2", mRandom.astring(10, 20).c_str()));
        assertOk(row->SetString("d_city", mRandom.astring(10, 20).c_str()));
        assertOk(row->SetString("d_state", mRandom.astring(2, 2).c_str()));
        assertOk(row->SetString("d_zip", mRandom.zipCode().c_str()));
        assertOk(row->SetInt32("d_tax", int(mRandom.randomWithin(0, 2000))));
        assertOk(row->SetInt64("d_ytd", int64_t(3000000)));
        assertOk(row->SetInt32("d_next_o_id", int(3001)));
        assertOk(session.Apply(ins));
        populateCustomers(session, w_id, i, n, useCH);
        populateOrders(session, i, w_id, n);
        populateNewOrders(session, w_id, i);
    }
    assertOk(session.Flush());
}

void Populator::populateCustomers(KuduSession &session,
                                  int16_t w_id, int16_t d_id,
                                  int64_t c_since, bool useCH) {
    std::tr1::shared_ptr<KuduTable> table;
    assertOk(session.client()->OpenTable("customer", &table));
    for (int32_t c_id = 1; c_id <= 3000; ++c_id) {
        std::string c_credit("GC");
        if (mRandom.randomWithin(0, 9) == 0) {
            c_credit = "BC";
        }
        int32_t rNum = c_id - 1;
        if (rNum >= 1000) {
            rNum = mRandom.NURand<int32_t>(255, 0, 999);
        }

        std::string c_last = mRandom.cLastName(rNum).c_str();
#ifndef NDEBUG
        // Check whether last name makes sense
        for (auto c : c_last) {
            switch (c) {
            case 'A':
            case 'B':
            case 'C':
            case 'D':
            case 'E':
            case 'F':
            case 'G':
            case 'H':
            case 'I':
            case 'J':
            case 'K':
            case 'L':
            case 'M':
            case 'N':
            case 'O':
            case 'P':
            case 'Q':
            case 'R':
            case 'S':
            case 'T':
            case 'U':
            case 'V':
            case 'W':
            case 'X':
            case 'Y':
            case 'Z':
                break;
            default:
                assert(false);
            }
        }
#endif
        std::string c_first = mRandom.astring(8, 16).c_str();
        auto ins = table->NewInsert();
        auto row = ins->mutable_row();
        assertOk(row->SetInt32("c_id", c_id));
        assertOk(row->SetInt16("c_d_id", d_id));
        assertOk(row->SetInt16("c_w_id", w_id));
        assertOk(row->SetString("c_first", c_first));
        assertOk(row->SetString("c_middle", "OE"));
        assertOk(row->SetString("c_last", c_last));
        assertOk(row->SetString("c_street_1", mRandom.astring(10, 20).c_str()));
        assertOk(row->SetString("c_street_2", mRandom.astring(10, 20).c_str()));
        assertOk(row->SetString("c_city", mRandom.astring(10, 20).c_str()));
        assertOk(row->SetString("c_state", mRandom.astring(2, 2).c_str()));
        assertOk(row->SetString("c_zip", mRandom.zipCode().c_str()));
        assertOk(row->SetString("c_phone", mRandom.nstring(16, 16).c_str()));
        assertOk(row->SetInt64("c_since", c_since));
        assertOk(row->SetString("c_credit", c_credit));
        assertOk(row->SetInt64("c_credit_lim", int64_t(5000000)));
        assertOk(row->SetInt32("c_discount", int(mRandom.randomWithin(0, 50000))));
        assertOk(row->SetInt64("c_balance", int64_t(-1000)));
        assertOk(row->SetInt64("c_ytd_payment", int64_t(1000)));
        assertOk(row->SetInt16("c_payment_cnt", int16_t(1)));
        assertOk(row->SetInt16("c_delivery_cnt", int16_t(0)));
        assertOk(row->SetString("c_data", mRandom.astring(300, 500).c_str()));
        if (useCH)
            assertOk(row->SetInt16("c_n_nationkey", int16_t(mRandom.randomWithin(0,24))));
        assertOk(session.Apply(ins));
        {
            // write index
            std::tr1::shared_ptr<KuduTable> table;
            assertOk(session.client()->OpenTable("c_last_idx", &table));
            auto ins = table->NewInsert();
            auto row = ins->mutable_row();
            assertOk(row->SetInt16("c_w_id", w_id));
            assertOk(row->SetInt16("c_d_id", d_id));
            assertOk(row->SetString("c_last", c_last));
            assertOk(row->SetString("c_first", c_first));
            assertOk(row->SetInt32("c_id", c_id));
            assertOk(session.Apply(ins));
        }
        populateHistory(session, c_id, d_id, w_id, c_since);
        assertOk(session.Flush());
    }
    assertOk(session.Flush());
}

void Populator::populateHistory(KuduSession &session,
                                int32_t c_id,
                                int16_t d_id, int16_t w_id, int64_t n) {
    std::tr1::shared_ptr<KuduTable> table;
    assertOk(session.client()->OpenTable("history", &table));
    auto ins = table->NewInsert();
    auto row = ins->mutable_row();
    assertOk(row->SetInt64("h_ts", std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count()));
    assertOk(row->SetInt32("h_c_id", c_id));
    assertOk(row->SetInt16("h_c_d_id", d_id));
    assertOk(row->SetInt16("h_c_w_id", w_id));
    assertOk(row->SetInt16("h_d_id", d_id));
    assertOk(row->SetInt16("h_w_id", w_id));
    assertOk(row->SetInt64("h_date", n));
    assertOk(row->SetInt32("h_amount", int32_t(1000)));
    assertOk(row->SetString("h_data", mRandom.astring(12, 24).c_str()));
    assertOk(session.Apply(ins));
}

void Populator::populateOrders(KuduSession &session, int16_t d_id,
                               int16_t w_id, int64_t o_entry_d) {
    std::tr1::shared_ptr<KuduTable> table;
    assertOk(session.client()->OpenTable("order", &table));
    std::vector<int32_t> c_ids(3000, 0);
    for (int32_t i = 0; i < 3000; ++i) {
        c_ids[i] = i + 1;
    }
    std::shuffle(c_ids.begin(), c_ids.end(), mRandom.randomDevice());
    for (int o_id = 1; o_id <= 3000; ++o_id) {
        auto o_ol_cnt = int16_t(mRandom.randomWithin(5, 15));
        auto ins = table->NewInsert();
        auto row = ins->mutable_row();
           assertOk(row->SetInt32("o_id", o_id));
           assertOk(row->SetInt16("o_d_id", d_id));
           assertOk(row->SetInt16("o_w_id", w_id));
           assertOk(row->SetInt32("o_c_id", c_ids[o_id - 1]));
           assertOk(row->SetInt64("o_entry_d", o_entry_d));
           assertOk(row->SetNull("o_carrier_id"));
           assertOk(row->SetInt16("o_ol_cnt", o_ol_cnt));
           assertOk(row->SetInt16("o_all_local", int16_t(1)));
        if (o_id <= 2100) {
           assertOk(row->SetInt16("o_carrier_id", mRandom.random<int16_t>(1, 10)));
        }
        assertOk(session.Apply(ins));
        {
            std::tr1::shared_ptr<KuduTable> table;
            assertOk(session.client()->OpenTable("order_idx", &table));
            auto ins = table->NewInsert();
            auto row = ins->mutable_row();
            assertOk(row->SetInt16("o_w_id", w_id));
            assertOk(row->SetInt16("o_d_id", d_id));
            assertOk(row->SetInt32("o_c_id", c_ids[o_id - 1]));
            assertOk(row->SetInt32("o_id", o_id));
            assertOk(session.Apply(ins));
        }
        populateOrderLines(session, o_id, d_id, w_id, o_ol_cnt, o_entry_d);
    }
    assertOk(session.Flush());
}

void Populator::populateOrderLines(KuduSession &session,
                                   int32_t o_id, int16_t d_id, int16_t w_id,
                                   int16_t ol_cnt, int64_t o_entry_d) {
    std::tr1::shared_ptr<KuduTable> table;
    assertOk(session.client()->OpenTable("order-line", &table));
    for (int16_t ol_number = 1; ol_number <= ol_cnt; ++ol_number) {
        auto ins = table->NewInsert();
        auto row = ins->mutable_row();
        assertOk(row->SetInt32("ol_o_id", o_id));
        assertOk(row->SetInt16("ol_d_id", d_id));
        assertOk(row->SetInt16("ol_w_id", w_id));
        assertOk(row->SetInt16("ol_number", ol_number));
        assertOk(row->SetInt32("ol_i_id", mRandom.random<int32_t>(1, 100000)));
        assertOk(row->SetInt16("ol_supply_w_id", w_id));
        if (o_id < 2101) {
            assertOk(row->SetInt64("ol_delivery_d", o_entry_d));
        } else {
            assertOk(row->SetNull("ol_delivery_d"));
        }
        assertOk(row->SetInt16("ol_quantity", int16_t(5)));
        assertOk(row->SetInt32("ol_amount", o_id < 2101
                    ? int32_t(0)
                    : mRandom.randomWithin<int32_t>(1, 999999)));
        assertOk(row->SetString("ol_dist_info", mRandom.astring(24, 24).c_str()));
        assertOk(session.Apply(ins));
    }
    assertOk(session.Flush());
}

void Populator::populateNewOrders(KuduSession &session,
                                  int16_t w_id, int16_t d_id) {
    std::tr1::shared_ptr<KuduTable> table;
    assertOk(session.client()->OpenTable("new-order", &table));
    for (int32_t o_id = 2101; o_id <= 3000; ++o_id) {
        auto ins = table->NewInsert();
        auto row = ins->mutable_row();
          assertOk(row->SetInt32("no_o_id", o_id));
          assertOk(row->SetInt16("no_d_id", d_id));
          assertOk(row->SetInt16("no_w_id", w_id));
        assertOk(session.Apply(ins));
    }
    assertOk(session.Flush());
}

} // namespace tpcc
