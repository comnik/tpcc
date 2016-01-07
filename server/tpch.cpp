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
#include "tpch.hpp"

#include <sstream>
#include <fstream>
#include <iomanip>

#include <boost/lexical_cast.hpp>
#include <boost/date_time.hpp>
#include <kudu/client/client.h>
#include <telldb/Transaction.hpp>
#include <telldb/TellDB.hpp>
#include <crossbow/logger.hpp>
#include <crossbow/program_options.hpp>

namespace tpch {

using namespace kudu::client;
using namespace tell::db;

void assertOk(kudu::Status status) {
    if (!status.ok()) {
        LOG_ERROR("ERROR from Kudu: %1%", status.message().ToString());
        throw std::runtime_error(status.message().ToString().c_str());
    }
}

template<class T>
struct TableCreator;

enum class type {
    SMALLINT, INT, BIGINT, FLOAT, DOUBLE, TEXT
};

template<>
struct TableCreator<kudu::client::KuduSession> {
    KuduSession& session;
    std::unique_ptr<KuduTableCreator> tableCreator;
    KuduSchemaBuilder schemaBuilder;
    TableCreator(KuduSession& session)
        : session(session)
        , tableCreator(session.client()->NewTableCreator())
    {
        tableCreator->num_replicas(1);
    }

    template<class S>
    void operator() (const S& name, type t, bool notNull = true) {
        auto col = schemaBuilder.AddColumn(name);
        switch (t) {
        case type::SMALLINT:
            col->Type(KuduColumnSchema::INT16);
            break;
        case type::INT:
            col->Type(KuduColumnSchema::INT32);
            break;
        case type::BIGINT:
            col->Type(KuduColumnSchema::INT64);
            break;
        case type::FLOAT:
            col->Type(KuduColumnSchema::FLOAT);
            break;
        case type::DOUBLE:
            col->Type(KuduColumnSchema::DOUBLE);
            break;
        case type::TEXT:
            col->Type(KuduColumnSchema::STRING);
            break;
        }
        if (notNull) {
            col->NotNull();
        } else {
            col->Nullable();
        }
    }

    void setPrimaryKey(const std::vector<std::string>& key) {
        schemaBuilder.SetPrimaryKey(key);
    }

    void create(const std::string& name) {
        KuduSchema schema;
        assertOk(schemaBuilder.Build(&schema));

        tableCreator->schema(&schema);
        tableCreator->table_name(name);
        tableCreator->Create();
    }
};

template<>
struct TableCreator<Transaction> {
    Transaction& tx;
    tell::store::Schema schema;

    TableCreator(Transaction& tx)
        : tx(tx)
        , schema(tell::store::TableType::TRANSACTIONAL)
    {}

    template<class S>
    void operator() (const S& name, type t, bool notNull = true) {
        using namespace tell::store;
        switch (t) {
        case type::SMALLINT:
            schema.addField(FieldType::SMALLINT, name, notNull);
            break;
        case type::INT:
            schema.addField(FieldType::INT, name, notNull);
            break;
        case type::BIGINT:
            schema.addField(FieldType::BIGINT, name, notNull);
            break;
        case type::FLOAT:
            schema.addField(FieldType::FLOAT, name, notNull);
            break;
        case type::DOUBLE:
            schema.addField(FieldType::DOUBLE, name, notNull);
            break;
        case type::TEXT:
            schema.addField(FieldType::TEXT, name, notNull);
            break;
        }
    }

    void create(const std::string& name) {
        tx.createTable(name, schema);
    }

    void setPrimaryKey(const std::vector<std::string>&) {
        // we do not explicitely set the primary key on tell
    }
};

template<class T>
void createPart(T& tx) {
    TableCreator<T> tc(tx);
    tc("P_PARTKEY", type::INT);
    tc("P_NAME", type::TEXT);
    tc("P_MFGR", type::TEXT);
    tc("P_BRAND", type::TEXT);
    tc("P_TYPE", type::TEXT);
    tc("P_SIZE", type::INT);
    tc("P_CONTAINER", type::TEXT);
    tc("P_RETAILPRICE", type::DOUBLE);
    tc("P_COMMENT", type::TEXT);
    tc.setPrimaryKey({"P_PARTKEY"});
    tc.create("PART");
}

template<class T>
void createSupplier(T& tx) {
    TableCreator<T> tc(tx);
    tc("S_SUPPKEY", type::INT);
    tc("S_NAME", type::TEXT);
    tc("S_ADDRESS", type::TEXT);
    tc("S_NATIONKEY", type::INT);
    tc("S_PHONE", type::TEXT);
    tc("S_ACCTBAL", type::DOUBLE);
    tc("S_COMMENT", type::TEXT);
    tc.setPrimaryKey({"S_SUPPKEY"});
    tc.create("SUPPLIER");
}

template<class T>
void createPartsupp(T& tx) {
    TableCreator<T> tc(tx);
    tc("PS_PARTKEY", type::INT);
    tc("PS_SUPPKEY", type::INT);
    tc("PS_AVAILQTY", type::INT);
    tc("PS_SUPPLYCOST", type::DOUBLE);
    tc("PS_COMMENT", type::TEXT);
    tc.setPrimaryKey({"PS_PARTKEY", "PS_SUPPKEY"});
    tc.create("PARTSUPP");
}

template<class T>
void createCustomer(T& tx) {
    TableCreator<T> tc(tx);
    tc("C_CUSTKEY", type::INT);
    tc("C_NAME", type::TEXT);
    tc("C_ADDRESS", type::TEXT);
    tc("C_NATIONKEY", type::INT);
    tc("C_PHONE", type::TEXT);
    tc("C_ACCTBAL", type::DOUBLE);
    tc("C_MKTSEGMENT", type::TEXT);
    tc("C_COMMENT", type::TEXT);
    tc.setPrimaryKey({"C_CUSTKEY"});
    tc.create("CUSTOMER");
}

template<class T>
void createOrder(T& tx) {
    TableCreator<T> tc(tx);
    tc("O_ORDERKEY", type::INT);
    tc("O_CUSTKEY", type::INT);
    tc("O_ORDERSTATUS", type::TEXT);
    tc("O_TOTALPRICE", type::DOUBLE);
    tc("O_ORDERDATE", type::BIGINT);
    tc("O_ORDERPRIORITY", type::TEXT);
    tc("O_CLERK", type::TEXT);
    tc("O_SHIPPRIORITY", type::INT);
    tc("O_COMMENT", type::TEXT);
    tc.setPrimaryKey({"O_ORDERKEY"});
    tc.create("ORDERS");
}

template<class T>
void createLineitem(T& tx) {
    TableCreator<T> tc(tx);
    tc("L_ORDERKEY", type::INT);
    tc("L_LINENUMBER", type::INT);
    tc("L_PARTKEY", type::INT);
    tc("L_SUPPKEY", type::INT);
    tc("L_QUANTITY", type::DOUBLE);
    tc("L_EXTENDEDPRICE", type::DOUBLE);
    tc("L_DISCOUNT", type::DOUBLE);
    tc("L_TAX", type::DOUBLE);
    tc("L_RETURNFLAG", type::TEXT);
    tc("L_LINESTATUS", type::TEXT);
    tc("L_SHIPDATE", type::BIGINT);
    tc("L_COMMITDATE", type::BIGINT);
    tc("L_RECEIPTDATE", type::BIGINT);
    tc("L_SHIPINSTRUCT", type::TEXT);
    tc("L_SHIPMODE", type::TEXT);
    tc("L_COMMENT", type::TEXT);
    tc.setPrimaryKey({"L_ORDERKEY", "L_LINENUMBER"});
    tc.create("LINEITEM");
}

template<class T>
void createNation(T& tx) {
    TableCreator<T> tc(tx);
    tc("N_NATIONKEY", type::INT);
    tc("N_NAME", type::TEXT);
    tc("N_REGIONKEY", type::INT);
    tc("N_COMMENT", type::TEXT);
    tc.setPrimaryKey({"N_NATIONKEY"});
    tc.create("NATION");
}

template<class T>
void createRegion(T& tx) {
    TableCreator<T> tc(tx);
    tc("R_REGIONKEY", type::INT);
    tc("R_NAME", type::TEXT);
    tc("R_COMMENT", type::TEXT);
    tc.setPrimaryKey({"R_REGIONKEY"});
    tc.create("REGION");
}

template<class T>
void createSchema(T& tx) {
    createPart(tx);
    createSupplier(tx);
    createPartsupp(tx);
    createCustomer(tx);
    createOrder(tx);
    createLineitem(tx);
    createNation(tx);
    createRegion(tx);
}

template void createSchema<kudu::client::KuduSession>(kudu::client::KuduSession&);
template void createSchema<tell::db::Transaction>(tell::db::Transaction&);

struct date {
    int64_t value;

    date() : value(0) {}

    date(const std::string& str) {
        using namespace boost::posix_time;
        ptime epoch(boost::gregorian::date(1970, 1, 1));
        auto time = time_from_string(str + " 00:00:00");
        value = (time - epoch).total_milliseconds();
    }

    operator int64_t() const {
        return value;
    }
};

template<class T>
struct Populator;

template<>
struct Populator<KuduSession> {
    KuduSession& session;
    std::tr1::shared_ptr<KuduTable> table;
    std::unique_ptr<KuduInsert> ins;
    kudu::KuduPartialRow* row;
    std::vector<std::string> names;
    std::vector<std::string> vals;
    Populator(KuduSession& session, const std::string& tableName)
        : session(session)
    {
        assertOk(session.client()->OpenTable(tableName, &table));
        ins.reset(table->NewInsert());
        row = ins->mutable_row();
    }

    template<class Str>
    void operator() (Str&& name, int16_t val) {
        names.emplace_back(std::forward<Str>(name));
        assertOk(row->SetInt16(names.back(), val));
    }

    template<class Str>
    void operator() (Str&& name, int32_t val) {
        names.emplace_back(std::forward<Str>(name));
        assertOk(row->SetInt32(names.back(), val));
    }

    template<class Str>
    void operator() (Str&& name, int64_t val) {
        names.emplace_back(std::forward<Str>(name));
        assertOk(row->SetInt64(names.back(), val));
    }

    template<class Str>
    void operator() (Str&& name, date d) {
        names.emplace_back(std::forward<Str>(name));
        assertOk(row->SetInt64(names.back(), d.value));
    }

    template<class Str>
    void operator() (Str&& name, float val) {
        names.emplace_back(std::forward<Str>(name));
        assertOk(row->SetFloat(names.back(), val));
    }

    template<class Str>
    void operator() (Str&& name, double val) {
        names.emplace_back(std::forward<Str>(name));
        assertOk(row->SetDouble(names.back(), val));
    }

    template<class Str1, class Str2>
    void operator() (Str1&& name, Str2&& val) {
        names.emplace_back(std::forward<Str1>(name));
        vals.emplace_back(std::forward<Str2>(val));
        assertOk(row->SetString(names.back(), vals.back()));
    }

    void apply(uint64_t k) {
        assertOk(session.Apply(ins.release()));
        ins.reset(table->NewInsert());
        row = ins->mutable_row();
    }

    void flush() {
        assertOk(session.Flush());
    }

    void commit() {
    }
};

template<>
struct Populator<Transaction> {
    Transaction& tx;
    std::unordered_map<crossbow::string, Field> fields;
    tell::db::table_t tableId;

    Populator(Transaction& tx, const crossbow::string& name)
        : tx(tx)
    {
        auto f = tx.openTable(name);
        tableId = f.get();
    }

    template<class Str>
    void operator() (Str&& name, int16_t val) {
        fields.emplace(std::forward<Str>(name), val);
    }

    template<class Str>
    void operator() (Str&& name, int32_t val) {
        fields.emplace(std::forward<Str>(name), val);
    }

    template<class Str>
    void operator() (Str&& name, int64_t val) {
        fields.emplace(std::forward<Str>(name), val);
    }

    template<class Str>
    void operator() (Str&& name, date d) {
        fields.emplace(std::forward<Str>(name), d.value);
    }

    template<class Str>
    void operator() (Str&& name, float val) {
        fields.emplace(std::forward<Str>(name), val);
    }

    template<class Str>
    void operator() (Str&& name, double val) {
        fields.emplace(std::forward<Str>(name), val);
    }

    template<class Str1, class Str2>
    void operator() (Str1&& name, Str2&& val) {
        fields.emplace(std::forward<Str1>(name), std::forward<Str2>(val));
    }

    void apply(uint64_t key) {
        tx.insert(tableId, tell::db::key_t{key}, fields);
    }

    void flush() {
        tx.unsafeFlush();
    }

    void commit() {
        tx.commit();
    }
};

template struct Populator<KuduSession>;
template struct Populator<Transaction>;


template<class Dest>
struct tpch_caster {
    Dest operator() (std::string&& str) const {
        return boost::lexical_cast<Dest>(str);
    }
};

template<>
struct tpch_caster<date> {
    date operator() (std::string&& str) const {
        return date(str);
    }
};

template<>
struct tpch_caster<std::string> {
    std::string operator() (std::string&& str) const {
        return std::move(str);
    }
};

template<>
struct tpch_caster<crossbow::string> {
    crossbow::string operator() (std::string&& str) const {
        return crossbow::string(str.begin(), str.end());
    }
};

template<class T, size_t P>
struct TupleWriter {
    TupleWriter<T, P - 1> next;
    void operator() (T& res, std::stringstream& ss) const {
        constexpr size_t total_size = std::tuple_size<T>::value;
        tpch_caster<typename std::tuple_element<total_size - P, T>::type> cast;
        std::string field;
        std::getline(ss, field, '|');
        std::get<total_size - P>(res) = cast(std::move(field));
        next(res, ss);
    }
};

template<class T>
struct TupleWriter<T, 0> {
    void operator() (T& res, std::stringstream& ss) const {
    }
};

template<class Tuple, class Fun>
void getFields(std::istream& in, Fun fun) {
    std::string line;
    Tuple tuple;
    std::vector<std::string> fields;
    TupleWriter<Tuple, std::tuple_size<Tuple>::value> writer;
    while (std::getline(in, line)) {
        std::stringstream ss(line);
        writer(tuple, ss);
        fun(tuple);
    }
}

template<class T>
struct string_type;

template<>
struct string_type<KuduSession> {
    using type = std::string;
};

template<>
struct string_type<Transaction> {
    using type = crossbow::string;
};

template<class T>
struct Populate {
    using string = typename string_type<T>::type;
    using P = Populator<T>;
    T& tx;

    Populate(T& tx)
        : tx(tx)
    {}

    void populatePart(std::istream& in) {
        using t = std::tuple<int32_t, string, string, string, string, int32_t, string, double, string>;
        P p(tx, "PART");
        int count = 0;
        getFields<t>(in,
            [&count, &p](const t& fields)
            {
                p("P_PARTKEY",     std::get<0>(fields));
                p("P_NAME",        std::get<1>(fields));
                p("P_MFGR",        std::get<2>(fields));
                p("P_BRAND",       std::get<3>(fields));
                p("P_TYPE",        std::get<4>(fields));
                p("P_SIZE",        std::get<5>(fields));
                p("P_CONTAINER",   std::get<6>(fields));
                p("P_RETAILPRICE", std::get<7>(fields));
                p("P_COMMENT",     std::get<8>(fields));
                p.apply(uint64_t(std::get<0>(fields)));
                if (++count % 1000 == 0)
                    p.flush();
            });
        p.flush();
    }

    void populateSupplier(std::istream& in) {
        using t = std::tuple<int32_t, string, string, int32_t, string, double, string>;
        P p(tx, "SUPPLIER");
        int count = 0;
        getFields<t>(in,
                [&count, &p](const t& fields) {
                    p("S_SUPPKEY",   std::get<0>(fields));
                    p("S_NAME",      std::get<1>(fields));
                    p("S_ADDRESS",   std::get<2>(fields));
                    p("S_NATIONKEY", std::get<3>(fields));
                    p("S_PHONE",     std::get<4>(fields));
                    p("S_ACCTBAL",   std::get<5>(fields));
                    p("S_COMMENT",   std::get<6>(fields));
                    p.apply(std::get<0>(fields));
                    if (++count % 1000 == 0)
                        p.flush();
                });
        p.flush();
    }

    void populatePartsupp(std::istream& in) {
        using t = std::tuple<int32_t, int32_t, int32_t, double, string>;
        P p(tx, "PARTSUPP");
        int count = 0;
        getFields<t>(in,
                [&count, &p](const t& fields) {
                    p("PS_PARTKEY",   std::get<0>(fields));
                    p("PS_SUPPKEY",   std::get<1>(fields));
                    p("PS_AVAILQTY",  std::get<2>(fields));
                    p("PS_SUPPLYCOST",std::get<3>(fields));
                    p("PS_COMMENT",   std::get<4>(fields));
                    p.apply((uint64_t(std::get<0>(fields)) << 32 | uint64_t(std::get<1>(fields))));
                    if (++count % 1000 == 0)
                        p.flush();
                });
        p.flush();
    }

    void populateCustomer(std::istream& in) {
        using t = std::tuple<int32_t, string, string, int32_t, string, double, string, string>;
        P p(tx, "CUSTOMER");
        int count = 0;
        getFields<t>(in,
                [&count, &p](const t& fields) {
                    p("C_CUSTKEY",    std::get<0>(fields));
                    p("C_NAME",       std::get<1>(fields));
                    p("C_ADDRESS",    std::get<2>(fields));
                    p("C_NATIONKEY",  std::get<3>(fields));
                    p("C_PHONE",      std::get<4>(fields));
                    p("C_ACCTBAL",    std::get<5>(fields));
                    p("C_MKTSEGMENT", std::get<6>(fields));
                    p("C_COMMENT",    std::get<7>(fields));
                    p.apply(std::get<0>(fields));
                    if (++count % 1000 == 0)
                        p.flush();
                });
        p.flush();
    }

    void populateOrder(std::istream& in) {
        using t = std::tuple<int32_t, int32_t, string, double, date, string, string, int32_t, string>;
        P p(tx, "ORDERS");
        int count = 0;
        getFields<t>(in,
                [&count, &p](const t& fields) {
                    p("O_ORDERKEY",     std::get<0>(fields));
                    p("O_CUSTKEY",      std::get<1>(fields));
                    p("O_ORDERSTATUS",  std::get<2>(fields));
                    p("O_TOTALPRICE",   std::get<3>(fields));
                    p("O_ORDERDATE",    std::get<4>(fields));
                    p("O_ORDERPRIORITY",std::get<5>(fields));
                    p("O_CLERK",        std::get<6>(fields));
                    p("O_SHIPPRIORITY", std::get<7>(fields));
                    p("O_COMMENT",      std::get<8>(fields));
                    p.apply(std::get<0>(fields));
                    if (++count % 1000 == 0)
                        p.flush();
                });
        p.flush();
    }

    void populateLineitem(std::istream& in) {
        using t = std::tuple<int32_t, int32_t, int32_t, int32_t, double, double, double, double, string, string, date, date, date, string, string, string>;
        P p(tx, "LINEITEM");
        int count = 0;
        getFields<t>(in,
                [&count, &p](const t& fields) {
                    p("L_ORDERKEY",      std::get<0>(fields));
                    p("L_PARTKEY",       std::get<1>(fields));
                    p("L_SUPPKEY",       std::get<2>(fields));
                    p("L_LINENUMBER",    std::get<3>(fields));
                    p("L_QUANTITY",      std::get<4>(fields));
                    p("L_EXTENDEDPRICE", std::get<5>(fields));
                    p("L_DISCOUNT",      std::get<6>(fields));
                    p("L_TAX",           std::get<7>(fields));
                    p("L_RETURNFLAG",    std::get<8>(fields));
                    p("L_LINESTATUS",    std::get<9>(fields));
                    p("L_SHIPDATE",      std::get<10>(fields));
                    p("L_COMMITDATE",    std::get<11>(fields));
                    p("L_RECEIPTDATE",   std::get<12>(fields));
                    p("L_SHIPINSTRUCT",  std::get<13>(fields));
                    p("L_SHIPMODE",      std::get<14>(fields));
                    p("L_COMMENT",       std::get<15>(fields));
                    uint64_t key = uint64_t(std::get<0>(fields)) << 32;
                    key |= uint64_t(std::get<3>(fields));
                    p.apply(key);
                    if (++count % 1000 == 0)
                        p.flush();
                });
        p.flush();
    }

    void populateNation(std::istream& in) {
        using t = std::tuple<int32_t, string, int32_t, string>;
        P p(tx, "NATION");
        int count = 0;
        getFields<t>(in,
                [&count, &p](const t& fields) {
                    p("N_NATIONKEY", std::get<0>(fields));
                    p("N_NAME",      std::get<1>(fields));
                    p("N_REGIONKEY", std::get<2>(fields));
                    p("N_COMMENT",   std::get<3>(fields));
                    p.apply(std::get<0>(fields));
                    if (++count % 1000 == 0)
                        p.flush();
                });
        p.flush();
    }

    void populateRegion(std::istream& in) {
        using t = std::tuple<int32_t, string, string>;
        P p(tx, "REGION");
        int count = 0;
        getFields<t>(in,
                [&count, &p](const t& fields) {
                    p("R_REGIONKEY", std::get<0>(fields));
                    p("R_NAME",      std::get<1>(fields));
                    p("R_COMMENT",   std::get<2>(fields));
                    p.apply(std::get<0>(fields));
                    if (++count % 1000 == 0)
                        p.flush();
                });
        p.flush();
    }

    void populate(const std::string& basedir) {
        {
            std::fstream in((basedir + "/part.tbl").c_str(), std::ios_base::in);
            populatePart(in);
        }
        {
            std::fstream in((basedir + "/partsupp.tbl").c_str(), std::ios_base::in);
            populatePartsupp(in);
        }
        {
            std::fstream in((basedir + "/supplier.tbl").c_str(), std::ios_base::in);
            populateSupplier(in);
        }
        {
            std::fstream in((basedir + "/customer.tbl").c_str(), std::ios_base::in);
            populateCustomer(in);
        }
        {
            std::fstream in((basedir + "/orders.tbl").c_str(), std::ios_base::in);
            populateOrder(in);
        }
        {
            std::fstream in((basedir + "/lineitem.tbl").c_str(), std::ios_base::in);
            populateLineitem(in);
        }
        {
            std::fstream in((basedir + "/nation.tbl").c_str(), std::ios_base::in);
            populateNation(in);
        }
        {
            std::fstream in((basedir + "/region.tbl").c_str(), std::ios_base::in);
            populateRegion(in);
        }
    }
};

template struct Populate<Transaction>;
template struct Populate<KuduSession>;

} // namespace tpch

using namespace crossbow::program_options;

int main(int argc, const char* argv[]) {
    bool help = false;
    bool use_kudu = false;
    std::string storage = "localhost";
    std::string commitManager;
    std::string baseDir = "/mnt/local/tell/tpch_2_17_0/dbgen";
    auto opts = create_options("tpch",
            value<'h'>("help", &help, tag::description{"print help"}),
            value<'S'>("storage", &storage, tag::description{"address(es) of the storage nodes"}),
            value<'C'>("commit-manager", &commitManager, tag::description{"address of the commit manager"}),
            value<'k'>("kudu", &use_kudu, tag::description{"use kudu instead of TellStore"}),
            value<'d'>("base-dir", &baseDir, tag::description{"Base directory to the generated tbl files"})
            );
    try {
        parse(opts, argc, argv);
    } catch (argument_not_found& ex) {
        std::cerr << ex.what() << std::endl;
        print_help(std::cout, opts);
        return 1;
    }
    if (help) {
        print_help(std::cout, opts);
        return 0;
    }

    if (use_kudu) {
        kudu::client::KuduClientBuilder clientBuilder;
        clientBuilder.add_master_server_addr(storage);
        std::tr1::shared_ptr<kudu::client::KuduClient> client;
        clientBuilder.Build(&client);
        auto session = client->NewSession();
        tpch::assertOk(session->SetFlushMode(kudu::client::KuduSession::MANUAL_FLUSH));
        session->SetTimeoutMillis(60000);
        tpch::createSchema(*session);
        tpch::Populate<kudu::client::KuduSession> populate(*session);
        populate.populate(baseDir);
        assertOk(session->Flush());
        assertOk(session->Close());
    } else {
        tell::store::ClientConfig clientConfig;
        clientConfig.tellStore = clientConfig.parseTellStore(storage);
        clientConfig.commitManager = crossbow::infinio::Endpoint(crossbow::infinio::Endpoint::ipv4(), commitManager.c_str());
        tell::db::ClientManager<void> clientManager(clientConfig);
        auto fun = [&baseDir](tell::db::Transaction& tx){
            tx.unsafeNoUndoLog();
            tpch::createSchema(tx);
            tpch::Populate<tell::db::Transaction> populate(tx);
            populate.populate(baseDir);
        };
        auto fiber = clientManager.startTransaction(fun, tell::store::TransactionType::READ_WRITE);
        fiber.wait();
    }
    return 0;
}

