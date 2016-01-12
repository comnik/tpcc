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
#include "TransactionsKudu.hpp"
#include "kudu.hpp"
#include <kudu/client/row_result.h>

#include <boost/unordered_map.hpp>

namespace tpcc {

using namespace kudu;
using namespace kudu::client;

using ScannerList = std::vector<std::unique_ptr<KuduScanner>>;

template<class T>
struct is_string {
    constexpr static bool value = false;
};

template<size_t S>
struct is_string<char[S]> {
    constexpr static bool value = true;
};

template<>
struct is_string<const char*> {
    constexpr static bool value = true;
};

template<>
struct is_string<std::string> {
    constexpr static bool value = true;
};

template<class T>
struct CreateValue {
    template<class U = T>
    typename std::enable_if<std::is_integral<U>::value, KuduValue*>::type
    create(U v) {
        return KuduValue::FromInt(v);
    }

    template<class U = T>
    typename std::enable_if<is_string<U>::value, KuduValue*>::type
    create(const U& v) {
        return KuduValue::CopyString(v);
    }
};


template<class... P>
struct PredicateAdder;

template<class Key, class Value, class... Tail>
struct PredicateAdder<Key, Value, Tail...> {
    PredicateAdder<Tail...> tail;
    CreateValue<Value> creator;

    void operator() (KuduTable& table, KuduScanner& scanner, const Key& key, const Value& value, const Tail&... rest) {
        assertOk(scanner.AddConjunctPredicate(table.NewComparisonPredicate(key, KuduPredicate::EQUAL, creator.template create<Value>(value))));
        tail(table, scanner, rest...);
    }
};

template<>
struct PredicateAdder<> {
    void operator() (KuduTable& table, KuduScanner& scanner) {
    }
};

template<class... Args>
void addPredicates(KuduTable& table, KuduScanner& scanner, const Args&... args) {
    PredicateAdder<Args...> adder;
    adder(table, scanner, args...);
}

template<class... Args>
KuduRowResult get(KuduTable& table, ScannerList& scanners, const Args&... args) {
    scanners.emplace_back(new KuduScanner(&table));
    auto& scanner = *scanners.back();
    addPredicates(table, scanner, args...);
    assertOk(scanner.Open());
    assert(scanner.HasMoreRows());
    std::vector<KuduRowResult> rows;
    assertOk(scanner.NextBatch(&rows));
    return rows[0];
}

void set(KuduWriteOperation& upd, const Slice& slice, int16_t v) {
    assertOk(upd.mutable_row()->SetInt16(slice, v));
}

void set(KuduWriteOperation& upd, const Slice& slice, int32_t v) {
    assertOk(upd.mutable_row()->SetInt32(slice, v));
}

void set(KuduWriteOperation& upd, const Slice& slice, int64_t v) {
    assertOk(upd.mutable_row()->SetInt64(slice, v));
}

void set(KuduWriteOperation& upd, const Slice& slice, std::nullptr_t) {
    assertOk(upd.mutable_row()->SetNull(slice));
}

void set(KuduWriteOperation& upd, const Slice& slice, const Slice& str) {
    assertOk(upd.mutable_row()->SetString(slice, str));
}

struct NewStock {
    int16_t s_w_id;
    int32_t s_i_id;
    int32_t s_quantity;
    int32_t s_ytd;
    int16_t s_order_cnt;
    int16_t s_remote_cnt;
};

NewOrderResult Transactions::newOrderTransaction(KuduSession& session, const NewOrderIn& in) {
    NewOrderResult result;
    std::tr1::shared_ptr<KuduTable> wTable;
    std::tr1::shared_ptr<KuduTable> cTable;
    std::tr1::shared_ptr<KuduTable> dTable;
    std::tr1::shared_ptr<KuduTable> oTable;
    std::tr1::shared_ptr<KuduTable> noTable;
    std::tr1::shared_ptr<KuduTable> iTable;
    std::tr1::shared_ptr<KuduTable> olTable;
    std::tr1::shared_ptr<KuduTable> sTable;
    assertOk(session.client()->OpenTable("warehouse", &wTable));
    assertOk(session.client()->OpenTable("customer", &cTable));
    assertOk(session.client()->OpenTable("district", &dTable));
    assertOk(session.client()->OpenTable("order", &oTable));
    assertOk(session.client()->OpenTable("new-order", &noTable));
    assertOk(session.client()->OpenTable("item", &iTable));
    assertOk(session.client()->OpenTable("order-line", &olTable));
    assertOk(session.client()->OpenTable("stock", &sTable));

    ScannerList scanners;
    auto warehouse = get(*wTable, scanners, "w_id", in.w_id);
    auto customer = get(*cTable, scanners, "c_w_id", in.w_id, "c_d_id", in.d_id, "c_id", in.c_id);
    auto district = get(*dTable, scanners, "d_w_id", in.w_id, "d_id", in.d_id);

    int32_t d_next_o_id;
    assertOk(district.GetInt32("d_next_o_id", &d_next_o_id));
    std::unique_ptr<KuduUpdate> update(dTable->NewUpdate());
    assertOk(update->mutable_row()->SetInt32("d_next_o_id", d_next_o_id + 1));
    assertOk(update->mutable_row()->SetInt16("d_w_id", in.w_id));
    assertOk(update->mutable_row()->SetInt16("d_id", in.d_id));
    assertOk(session.Apply(update.release()));

    auto o_id = d_next_o_id;
    int16_t o_all_local = 1;
    int16_t o_ol_cnt = rnd.randomWithin<int16_t>(5, 15);
    std::vector<int16_t> ol_supply_w_id(o_ol_cnt);
    for (auto& i : ol_supply_w_id) {
        i = in.w_id;
        if (mNumWarehouses > 1 && rnd.randomWithin<int>(1, 100) == 1) {
            o_all_local = 0;
            while (i == in.w_id) {
                i = rnd.randomWithin<int16_t>(1, mNumWarehouses);
            }
        }
    }
    std::vector<std::unique_ptr<KuduWriteOperation>> operations;
    auto datetime = now();
    std::unique_ptr<KuduInsert> ins(oTable->NewInsert());
    set(*ins, "o_id", o_id);
    set(*ins, "o_d_id", in.d_id);
    set(*ins, "o_w_id", in.w_id);
    set(*ins, "o_c_id", in.c_id);
    set(*ins, "o_entry_d", datetime);
    set(*ins, "o_carrier_id", nullptr);
    set(*ins, "o_ol_cnt", o_ol_cnt);
    set(*ins, "o_all_local", o_all_local);
    operations.emplace_back(ins.release());

    ins.reset(noTable->NewInsert());
    set(*ins, "no_o_id", o_id);
    set(*ins, "no_d_id", in.d_id);
    set(*ins, "no_w_id", in.w_id);
    operations.emplace_back(ins.release());

    std::vector<int32_t> ol_i_id;
    ol_i_id.reserve(o_ol_cnt);
    for (int16_t i = 0; i < o_ol_cnt; ++i) {
        auto i_id = rnd.NURand<int32_t>(8191,1,100000);
        ol_i_id.push_back(i_id);
    }
    // get the items
    // get the stocks
    boost::unordered_map<int32_t, KuduRowResult> items;
    boost::unordered_map<std::tuple<int16_t, int32_t>, KuduRowResult> stocks;
    items.reserve(o_ol_cnt);
    stocks.reserve(o_ol_cnt);
    for (int16_t i = 0; i < o_ol_cnt; ++i) {
        auto i_id = ol_i_id[i];
        if (items.count(i_id) == 0) {
            items.emplace(i_id, get(*iTable, scanners, "i_id", i_id));
        }
        auto sKey = std::make_tuple(ol_supply_w_id[i], i_id);
        if (stocks.count(sKey) == 0) {
            stocks.emplace(sKey, get(*sTable, scanners, "s_w_id", std::get<0>(sKey), "s_i_id", std::get<1>(sKey)));
        }
    }
    boost::unordered_map<std::tuple<int16_t, int32_t>, NewStock> newStocks;
    for (auto& p : stocks) {
        auto& stock = p.second;
        NewStock nStock;
        assertOk(stock.GetInt16("s_w_id", &nStock.s_w_id));
        assertOk(stock.GetInt32("s_i_id", &nStock.s_i_id));
        assertOk(stock.GetInt32("s_quantity", &nStock.s_quantity));
        assertOk(stock.GetInt32("s_ytd", &nStock.s_ytd));
        assertOk(stock.GetInt16("s_order_cnt", &nStock.s_order_cnt));
        assertOk(stock.GetInt16("s_remote_cnt", &nStock.s_remote_cnt));
        newStocks.emplace(p.first, std::move(nStock));
    }
    // set ol_dist_info key
    std::string ol_dist_info_key;
    if (in.d_id == 10) {
        ol_dist_info_key ="s_dist_10";
    } else {
        ol_dist_info_key = "s_dist_0" + std::to_string(in.d_id);
    }
    int32_t ol_amount_sum = 0;
    // insert the order lines
    std::vector<std::string> strings;
    for (int16_t i = 0; i < o_ol_cnt; ++i) {
        int16_t ol_number = i + 1;
        //ItemKey itemId(ol_i_id[i]);
        auto& item = items.at(ol_i_id[i]);
        auto stockId = std::make_tuple(ol_supply_w_id[i], ol_i_id[i]);
        auto& stock = stocks.at(stockId);
        kudu::Slice ol_dist_info_slice;
        assertOk(stock.GetString(ol_dist_info_key, &ol_dist_info_slice));
        strings.emplace_back(ol_dist_info_slice.ToString());
        auto& ol_dist_info = strings.back();
        auto ol_quantity = rnd.randomWithin<int16_t>(1, 10);
        auto& newStock = newStocks.at(stockId);
        if (newStock.s_quantity > ol_quantity + 10) {
            newStock.s_quantity -= ol_quantity;
        } else {
            newStock.s_quantity = (newStock.s_quantity - ol_quantity) + 91;
        }
        newStock.s_ytd += ol_quantity;
        ++newStock.s_order_cnt;
        if (ol_supply_w_id[i] != in.w_id) ++newStock.s_remote_cnt;
        int32_t i_price;
        assertOk(item.GetInt32("i_price", &i_price));
        int32_t ol_amount = i_price * int32_t(ol_quantity);
        ol_amount_sum += ol_amount;
        //auto olKey = std::make_tuple(w_id, d_id, o_id, ol_number);
        ins.reset(olTable->NewInsert());
        set(*ins, "ol_o_id", o_id);
        set(*ins, "ol_d_id", in.d_id);
        set(*ins, "ol_w_id", in.w_id);
        set(*ins, "ol_number", ol_number);
        set(*ins, "ol_i_id", ol_i_id[i]);
        set(*ins, "ol_supply_w_id", ol_supply_w_id[i]);
        set(*ins, "ol_delivery_d", nullptr);
        set(*ins, "ol_quantity", ol_quantity);
        set(*ins, "ol_amount", ol_amount);
        set(*ins, "ol_dist_info", ol_dist_info);
        // set Result for this order line
        Slice i_data, s_data;
        assertOk(item.GetString("i_data", &i_data));
        assertOk(stock.GetString("s_data", &s_data));
        NewOrderResult::OrderLine lineRes;
        lineRes.ol_supply_w_id = ol_supply_w_id[i];
        lineRes.ol_i_id = ol_i_id[i];
        Slice i_name;
        assertOk(item.GetString("i_name", &i_name));
        lineRes.i_name = i_name.ToString();
        lineRes.ol_quantity = ol_quantity;
        lineRes.s_quantity = newStock.s_quantity;
        lineRes.i_price = i_price;
        lineRes.ol_amount = ol_amount;
        lineRes.brand_generic = 'G';
        auto i_data_str = i_data.ToString();
        auto s_data_str = s_data.ToString();
        if (i_data_str.find("ORIGINAL") != i_data_str.npos && s_data_str.find("ORIGINAL") != s_data_str.npos) {
            lineRes.brand_generic = 'B';
        }
        result.lines.emplace_back(std::move(lineRes));
    }
    // update stock-entries
    for (const auto& p : stocks) {
        const auto& nStock = newStocks.at(p.first);
        std::unique_ptr<KuduUpdate> upd(sTable->NewUpdate());
        set(*upd, "s_w_id", nStock.s_w_id);
        set(*upd, "s_i_id", nStock.s_i_id);
        set(*upd, "s_quantity", nStock.s_quantity);
        set(*upd, "s_quantity", nStock.s_quantity);
        set(*upd, "s_ytd", nStock.s_ytd);
        set(*upd, "s_order_cnt", nStock.s_order_cnt);
        set(*upd, "s_remote_cnt", nStock.s_remote_cnt);
        operations.emplace_back(std::move(upd));
    }
    // 1% of transactions need to abort
    if (rnd.randomWithin<int>(1, 100) == 1) {
        result.success = false;
        result.error = "Item number is not valid";
        result.lines.clear();
    } else {
        // write single-line results
        result.o_id = o_id;
        result.o_ol_cnt = o_ol_cnt;
        Slice c_last, c_credit;
        assertOk(customer.GetString("c_last", &c_last));
        assertOk(customer.GetString("c_credit", &c_credit));
        result.c_last = c_last.ToString();
        result.c_credit = c_credit.ToString();
        assertOk(customer.GetInt32("c_discount", &result.c_discount));
        assertOk(warehouse.GetInt32("w_tax", &result.w_tax));
        assertOk(district.GetInt32("d_tax", &result.d_tax));
        result.o_entry_d = datetime;
        result.total_amount = ol_amount_sum * (1 - result.c_discount) * (1 + result.w_tax + result.d_tax);
    }

    if (result.success) {
        for (auto& op : operations) {
            assertOk(session.Apply(op.release()));
        }
        assertOk(session.Flush());
    }
    return result;
}

struct CustomerKey {
    int16_t c_w_id;
    int16_t c_d_id;
    int32_t c_id;
};

KuduRowResult getCustomer(KuduSession& session,
        ScannerList& scanners,
        bool selectByLastName,
        const crossbow::string& c_last_str,
        int16_t c_w_id,
        int16_t c_d_id,
        int32_t c_id,
        std::tr1::shared_ptr<KuduTable>& customerTable,
        std::tr1::shared_ptr<KuduTable>& idxTable,
        CustomerKey& customerKey)
{
    std::string c_last(c_last_str.begin(), c_last_str.end());
    scanners.emplace_back(new KuduScanner(customerTable.get()));
    auto& scanner = *scanners.back();
    if (selectByLastName) {
        addPredicates(*idxTable, scanner, "c_w_id", c_w_id, "c_d_id", c_d_id, "c_last", c_last);
        scanner.Open();
        std::vector<KuduRowResult> rows;
        std::vector<CustomerKey> keys;
        while (scanner.HasMoreRows()) {
            scanner.NextBatch(&rows);
            for (auto& row : rows) {
                Slice slice;
                assertOk(row.GetString("c_last", &slice));
                if (slice != c_last.c_str()) {
                    break;
                }
                keys.emplace_back();
                auto& key = keys.back();
                assertOk(row.GetInt16("c_w_id", &key.c_w_id));
                assertOk(row.GetInt16("c_d_id", &key.c_d_id));
                assertOk(row.GetInt32("c_id", &key.c_id));
                assert(key.c_id > 0);
            }
        }
        if (keys.empty()) {
            crossbow::string msg = "No customer found for name ";
            msg.append(c_last);
            throw std::runtime_error(msg.c_str());
        }
        auto pos = keys.size();
        pos = pos % 2 == 0 ? (pos / 2) : (pos / 2 + 1);
        customerKey = keys.at(pos - 1);
        assert(customerKey.c_id > 0);
    } else {
        customerKey = CustomerKey{c_w_id, c_d_id, c_id};
    }
    return get(*customerTable, scanners, "c_w_id", customerKey.c_w_id, "c_d_id", customerKey.c_d_id, "c_id", customerKey.c_id);
}

struct OrderKey {
    int16_t o_w_id, o_d_id;
    int32_t o_id;
};

OrderStatusResult Transactions::orderStatus(KuduSession& session, const OrderStatusIn& in) {
    OrderStatusResult result;
    std::tr1::shared_ptr<KuduTable> cTable;
    std::tr1::shared_ptr<KuduTable> oTable;
    std::tr1::shared_ptr<KuduTable> olTable;
    std::tr1::shared_ptr<KuduTable> idxTable;
    std::tr1::shared_ptr<KuduTable> idxOTable;
    session.client()->OpenTable("order", &oTable);
    session.client()->OpenTable("order-line", &olTable);
    session.client()->OpenTable("customer", &cTable);
    session.client()->OpenTable("c_last_idx", &idxTable);
    session.client()->OpenTable("order_idx", &idxOTable);
    // get Customer
    CustomerKey cKey{0, 0, 0};
    ScannerList scanners;
    try {
        getCustomer(session,
                scanners,
                in.selectByLastName,
                in.c_last,
                in.w_id,
                in.d_id,
                in.c_id,
                cTable,
                idxTable,
                cKey);
        // get newest order
        scanners.emplace_back(new KuduScanner(idxOTable.get()));
        auto& oScanner = *scanners.back();
        addPredicates(*idxOTable, oScanner, "o_w_id", in.w_id, "o_d_id", in.d_id, "o_c_id", cKey.c_id);
        // we need to get the last one - since Kudu does not support reverse iteration,
        // we need to iterate through all orders
        oScanner.Open();
        bool found = false;
        std::vector<KuduRowResult> rows;
        OrderKey oKey{in.w_id, in.d_id};
        while (oScanner.HasMoreRows()) {
            found = true;
            oScanner.NextBatch(&rows);
            assertOk(rows.back().GetInt32("o_id", &oKey.o_id));
        }
        if (!found) {
            result.success = false;
            std::stringstream errstream;
            errstream << "Customer name=" << in.c_last << ", w_id=" << in.w_id << ", d_id=" << in.d_id << ", c_id="
                << cKey.c_id << " does not exist";
            result.error = errstream.str();
            return result;
        }
        auto order = get(*oTable, scanners, "o_w_id", oKey.o_w_id, "o_d_id", oKey.o_d_id, "o_id", oKey.o_id);
        int16_t ol_cnt;
        assertOk(order.GetInt16("o_ol_cnt", &ol_cnt));
        // To get the order lines, we could use an index - but this is not necessary,
        // since we can generate all primary keys instead
        for (decltype(ol_cnt) i = 1; i <= ol_cnt; ++i) {
            auto ol_number = i;
            get(*olTable, scanners, "ol_w_id", in.w_id, "ol_d_id", in.d_id, "ol_o_id", oKey.o_id, "ol_number", ol_number);
        }
        result.success = true;
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }
    return result;
}

PaymentResult Transactions::payment(KuduSession& session, const PaymentIn& in) {
    PaymentResult result;
    result.success = true;
    std::vector<std::string> strings;
    try {
        std::tr1::shared_ptr<KuduTable> hTable;
        std::tr1::shared_ptr<KuduTable> dTable;
        std::tr1::shared_ptr<KuduTable> wTable;
        std::tr1::shared_ptr<KuduTable> cTable;
        std::tr1::shared_ptr<KuduTable> idxCTable;
        session.client()->OpenTable("customer", &cTable);
        session.client()->OpenTable("warehouse", &wTable);
        session.client()->OpenTable("district", &dTable);
        session.client()->OpenTable("history", &hTable);
        session.client()->OpenTable("c_last_idx", &idxCTable);
        CustomerKey customerKey{0, 0, 0};
        ScannerList scanners;
        std::vector<std::unique_ptr<KuduWriteOperation>> operations;
        auto customer = getCustomer(session, scanners, in.selectByLastName, in.c_last,
               in.c_w_id, in.c_d_id, in.c_id, cTable, idxCTable, customerKey);
        auto district = get(*dTable, scanners, "d_w_id", in.w_id, "d_id", in.d_id);
        auto warehouse = get(*wTable, scanners, "w_id", in.w_id);

        std::unique_ptr<KuduWriteOperation> upd(wTable->NewUpdate());
        set(*upd, "w_id", in.w_id);
        // update the warehouses ytd
        {
            int64_t value;
            assertOk(warehouse.GetInt64("w_ytd", &value));
            set(*upd, "w_ytd", int64_t(value + in.h_amount));
        }
        operations.emplace_back(upd.release());
        upd.reset(dTable->NewUpdate());
        set(*upd, "d_w_id", in.w_id);
        set(*upd, "d_id", in.d_id);
        {
            int64_t value;
            assertOk(district.GetInt64("d_ytd", &value));
            set(*upd, "d_ytd", int64_t(value + in.h_amount));
        }
        operations.emplace_back(upd.release());
        upd.reset(cTable->NewUpdate());
        int16_t c_w_id, c_d_id;
        int32_t c_id;
        {
            int64_t c_balance, c_ytd_payment;
            int16_t c_payment_cnt;
            Slice c_credit;
            assertOk(customer.GetInt16("c_w_id", &c_w_id));
            assertOk(customer.GetInt16("c_d_id", &c_d_id));
            assertOk(customer.GetInt32("c_id", &c_id));
            assertOk(customer.GetInt64("c_balance", &c_balance));
            assertOk(customer.GetInt64("c_ytd_payment", &c_ytd_payment));
            assertOk(customer.GetInt16("c_payment_cnt", &c_payment_cnt));
            assertOk(customer.GetInt64("c_balance", &c_balance));
            assertOk(customer.GetString("c_credit", &c_credit));

            set(*upd, "c_w_id", c_w_id);
            set(*upd, "c_d_id", c_d_id);
            set(*upd, "c_id", c_id);
            set(*upd, "c_balance", int64_t(c_balance + in.h_amount));
            set(*upd, "c_ytd_payment", int64_t(c_ytd_payment + in.h_amount));
            set(*upd, "c_payment_cnt", int16_t(c_payment_cnt + in.h_amount));
            if (c_credit == "BC") {
                std::string histInfo = "(" + std::to_string(customerKey.c_id) +
                    "," + std::to_string(c_d_id) + "," + std::to_string(c_w_id) +
                    "," + std::to_string(in.d_id) + "," + std::to_string(in.w_id) +
                    "," + std::to_string(in.h_amount);
                Slice c_data_str;
                assertOk(customer.GetString("c_data", &c_data_str));
                strings.emplace_back(c_data_str.ToString());
                auto& c_data = strings.back();
                c_data.insert(0, histInfo);
                if (c_data.size() > 500) {
                    c_data.resize(500);
                }
                set(*upd, "c_data", c_data);
            }
        }
        operations.emplace_back(upd.release());
        // insert into history
        Slice w_name, d_name;
        assertOk(warehouse.GetString("w_name", &w_name));
        assertOk(district.GetString("d_name", &d_name));
        strings.emplace_back(w_name.ToString() + d_name.ToString());
        auto& h_data = strings.back();
        std::unique_ptr<KuduInsert> ins(hTable->NewInsert());

        set(*ins, "h_ts", int64_t(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()));
        set(*ins, "h_c_id", c_id);
        set(*ins, "h_c_d_id", c_d_id);
        set(*ins, "h_c_w_id", c_w_id);
        set(*ins, "h_d_id", in.d_id);
        set(*ins, "h_w_id", in.w_id);
        set(*ins, "h_date", now());
        set(*ins, "h_amount", in.h_amount);
        set(*ins, "h_data", h_data);
        operations.emplace_back(ins.release());
        if (result.success) {
            for (auto& op : operations) {
                assertOk(session.Apply(op.release()));
            }
            assertOk(session.Flush());
        }
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }
    return result;
}

template<class F, class L>
void measureOpen(KuduScanner& scanner, F file, L line) {
    auto begin = std::chrono::system_clock::now();
    scanner.Open();
    auto time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - begin).count();
    std::cout << "Scan open took " << time << " ms (" << file << ":" << line << ")\n";
}

DeliveryResult Transactions::delivery(KuduSession& session, const DeliveryIn& in) {
    DeliveryResult result;
    try {
        std::tr1::shared_ptr<KuduTable> cTable;
        std::tr1::shared_ptr<KuduTable> olTable;
        std::tr1::shared_ptr<KuduTable> oTable;
        std::tr1::shared_ptr<KuduTable> noTable;
        session.client()->OpenTable("new-order", &noTable);
        session.client()->OpenTable("order", &oTable);
        session.client()->OpenTable("order-line", &olTable);
        session.client()->OpenTable("customer", &cTable);
        auto ol_delivery_d = now();
        std::vector<std::unique_ptr<KuduWriteOperation>> operations;
        std::vector<KuduRowResult> rows;
        result.success = true;
        for (int16_t d_id = 1; d_id <= 10; ++d_id) {
            ScannerList scanners;
            scanners.emplace_back(new KuduScanner(noTable.get()));
            auto& scanner = *scanners.back();
            addPredicates(*noTable, scanner, "no_w_id", in.w_id, "no_d_id", d_id);
            scanner.Open();
            assert(scanner.HasMoreRows());
            scanner.NextBatch(&rows);
            auto& newOrder = rows[0];
            ScannerList localScanners;
            int32_t no_o_id;
            assertOk(newOrder.GetInt32("no_o_id", &no_o_id));

            std::unique_ptr<KuduDelete> del(noTable->NewDelete());
            set(*del, "no_w_id", in.w_id);
            set(*del, "no_d_id", d_id);
            set(*del, "no_o_id", no_o_id);
            operations.emplace_back(del.release());

            int32_t c_id;
            auto order = get(*oTable, localScanners, "o_w_id", in.w_id, "o_d_id", d_id, "o_id", no_o_id);
            assertOk(order.GetInt32("o_c_id", &c_id));
            std::unique_ptr<KuduUpdate> upd(oTable->NewUpdate());
            set(*upd, "o_w_id", in.w_id);
            set(*upd, "o_d_id", d_id);
            set(*upd, "o_id", no_o_id);
            set(*upd, "o_carrier_id", in.o_carrier_id);
            operations.emplace_back(upd.release());

            int16_t o_ol_cnt;
            assertOk(order.GetInt16("o_ol_cnt", &o_ol_cnt));
            localScanners.emplace_back(new KuduScanner(olTable.get()));
            auto& olScanner = localScanners.back();
            addPredicates(*olTable, *olScanner, "ol_w_id", in.w_id, "ol_d_id", d_id, "ol_o_id", no_o_id);
            olScanner->Open();
            std::vector<KuduRowResult> ols;
            int64_t amount = 0;
            while (olScanner->HasMoreRows()) {
                olScanner->NextBatch(&ols);
                for (auto& ol : ols) {
                    int16_t ol_number;
                    int32_t ol_amount;
                    assertOk(ol.GetInt16("ol_number", &ol_number));
                    assertOk(ol.GetInt32("ol_amount", &ol_amount));
                    amount += ol_amount;

                    upd.reset(olTable->NewUpdate());
                    set(*upd, "ol_w_id", in.w_id);
                    set(*upd, "ol_d_id", d_id);
                    set(*upd, "ol_o_id", no_o_id);
                    set(*upd, "ol_number", ol_number);
                    set(*upd, "ol_delivery_d", ol_delivery_d);
                    operations.emplace_back(upd.release());
                }
            }

            auto customer = get(*cTable, localScanners, "c_w_id", in.w_id, "c_d_id", d_id, "c_id", c_id);
            int64_t c_balance;
            int16_t c_delivery_cnt;
            assertOk(customer.GetInt64("c_balance", &c_balance));
            assertOk(customer.GetInt16("c_delivery_cnt", &c_delivery_cnt));
            c_balance += amount;
            c_delivery_cnt += 1;

            upd.reset(cTable->NewUpdate());
            set(*upd, "c_w_id", in.w_id);
            set(*upd, "c_d_id", d_id);
            set(*upd, "c_id", c_id);
            set(*upd, "c_balance", c_balance);
            set(*upd, "c_delivery_cnt", c_delivery_cnt);
            operations.emplace_back(upd.release());
        }
        if (result.success) {
            for (auto& op : operations) {
                assertOk(session.Apply(op.release()));
            }
            assertOk(session.Flush());
        }
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }
    return result;
}

StockLevelResult Transactions::stockLevel(KuduSession& session, const StockLevelIn& in) {
    StockLevelResult result;
    result.low_stock = 0;
    try {
        std::tr1::shared_ptr<KuduTable> sTable;
        std::tr1::shared_ptr<KuduTable> olTable;
        std::tr1::shared_ptr<KuduTable> oTable;
        std::tr1::shared_ptr<KuduTable> dTable;
        session.client()->OpenTable("district", &dTable);
        session.client()->OpenTable("order", &oTable);
        session.client()->OpenTable("order-line", &olTable);
        session.client()->OpenTable("stock", &sTable);

        ScannerList scanners;
        // get District
        auto district = get(*dTable, scanners, "d_w_id", in.w_id, "d_id", in.d_id);
        int32_t d_next_o_id;
        assertOk(district.GetInt32("d_next_o_id", &d_next_o_id));

        scanners.emplace_back(new KuduScanner(olTable.get()));
        auto& olScanner = scanners.back();
        addPredicates(*olTable, *olScanner, "ol_w_id", in.w_id, "ol_d_id", in.d_id);
        assertOk(olScanner->AddConjunctPredicate(olTable->NewComparisonPredicate("ol_o_id", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(d_next_o_id - 20))));
        olScanner->Open();

        // count low_stock
        result.low_stock = 0;
        std::vector<KuduRowResult> rows;
        std::unique_ptr<KuduScanner> scanner;
        while (olScanner->HasMoreRows()) {
            olScanner->NextBatch(&rows);
            for (auto& row : rows) {
                int32_t ol_i_id;
                assertOk(row.GetInt32("ol_i_id", &ol_i_id));
                scanner.reset(new KuduScanner(sTable.get()));
                addPredicates(*sTable, *scanner, "s_w_id", in.w_id, "s_i_id", ol_i_id);
                scanner->Open();
                std::vector<KuduRowResult> stocks;
                while (scanner->HasMoreRows()) {
                    scanner->NextBatch(&stocks);
                    for (auto& stock : stocks) {
                        int32_t s_quantity;
                        assertOk(stock.GetInt32("s_quantity", &s_quantity));
                        if (s_quantity < in.threshold) {
                            ++result.low_stock;
                        }
                    }
                }
            }
        }
        result.success = true;
        return result;
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }
    return result;
}

} // namespace tpcc
