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
#include "CreateSchemaKudu.hpp"
#include "kudu.hpp"

namespace tpcc {

namespace {

enum class FieldType {
    SMALLINT, INT, BIGINT, FLOAT, DOUBLE, TEXT
};

using namespace kudu::client;

void addField(kudu::client::KuduSchemaBuilder& schemaBuilder, FieldType type, const std::string& name, bool notNull) {
    auto col = schemaBuilder.AddColumn(name);
    switch (type) {
    case FieldType::SMALLINT:
        col->Type(kudu::client::KuduColumnSchema::INT16);
        break;
    case FieldType::INT:
        col->Type(kudu::client::KuduColumnSchema::INT32);
        break;
    case FieldType::BIGINT:
        col->Type(kudu::client::KuduColumnSchema::INT64);
        break;
    case FieldType::FLOAT:
        col->Type(kudu::client::KuduColumnSchema::FLOAT);
        break;
    case FieldType::DOUBLE:
        col->Type(kudu::client::KuduColumnSchema::DOUBLE);
        break;
    case FieldType::TEXT:
        col->Type(kudu::client::KuduColumnSchema::STRING);
        break;
    }
    if (notNull) {
        col->NotNull();
    } else {
        col->Nullable();
    }
}

void createWarehouse(kudu::client::KuduSession& session) {
    // Warehouse table
    // w_id is used as primary key (since we are not issuing range queries)
    // w_id is a 16 bit number
    std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
    tableCreator->num_replicas(1);
    kudu::client::KuduSchemaBuilder schemaBuilder;

    addField(schemaBuilder, FieldType::SMALLINT, "w_id", true);
    addField(schemaBuilder, FieldType::TEXT, "w_name", true);
    addField(schemaBuilder, FieldType::TEXT, "w_street_1", true);
    addField(schemaBuilder, FieldType::TEXT, "w_street_2", true);
    addField(schemaBuilder, FieldType::TEXT, "w_city", true);
    addField(schemaBuilder, FieldType::TEXT, "w_state", true);
    addField(schemaBuilder, FieldType::TEXT, "w_zip", true);
    addField(schemaBuilder, FieldType::INT, "w_tax", true);          //numeric (4,4)
    addField(schemaBuilder, FieldType::BIGINT, "w_ytd", true);       //numeric (12,2)
    schemaBuilder.SetPrimaryKey({"w_id"});

    kudu::client::KuduSchema schema;
    assertOk(schemaBuilder.Build(&schema));
    tableCreator->schema(&schema);
    tableCreator->table_name("warehouse");
    tableCreator->Create();
}

void createDistrict(KuduSession& session) {
    std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
    tableCreator->num_replicas(1);
    kudu::client::KuduSchemaBuilder schemaBuilder;
    // Primary key: (d_w_id, d_id)
    //              ( 2 b    1 byte
    addField(schemaBuilder, FieldType::SMALLINT, "d_w_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "d_id", true);
    addField(schemaBuilder, FieldType::TEXT, "d_name", true);
    addField(schemaBuilder, FieldType::TEXT, "d_street_1", true);
    addField(schemaBuilder, FieldType::TEXT, "d_street_2", true);
    addField(schemaBuilder, FieldType::TEXT, "d_city", true);
    addField(schemaBuilder, FieldType::TEXT, "d_state", true);
    addField(schemaBuilder, FieldType::TEXT, "d_zip", true);
    addField(schemaBuilder, FieldType::INT, "d_tax", true);          //numeric (4,4)
    addField(schemaBuilder, FieldType::BIGINT, "d_ytd", true);       //numeric (12,2)
    addField(schemaBuilder, FieldType::INT, "d_next_o_id", true);
    schemaBuilder.SetPrimaryKey({"d_w_id", "d_id"});

    kudu::client::KuduSchema schema;
    assertOk(schemaBuilder.Build(&schema));
    tableCreator->schema(&schema);
    tableCreator->table_name("district");
    tableCreator->Create();
}

void createCustomer(KuduSession& session, bool useCH) {
    // Primary key: (c_w_id, c_d_id, c_id)
    // c_w_id: 2 bytes
    // c_d_id: 1 byte
    // c_id: 4 bytes
    std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
    tableCreator->num_replicas(1);
    kudu::client::KuduSchemaBuilder schemaBuilder;

    addField(schemaBuilder, FieldType::SMALLINT, "c_w_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "c_d_id", true);
    addField(schemaBuilder, FieldType::INT, "c_id", true);
    addField(schemaBuilder, FieldType::TEXT, "c_first", true);
    addField(schemaBuilder, FieldType::TEXT, "c_middle", true);
    addField(schemaBuilder, FieldType::TEXT, "c_last", true);
    addField(schemaBuilder, FieldType::TEXT, "c_street_1", true);
    addField(schemaBuilder, FieldType::TEXT, "c_street_2", true);
    addField(schemaBuilder, FieldType::TEXT, "c_city", true);
    addField(schemaBuilder, FieldType::TEXT, "c_state", true);
    addField(schemaBuilder, FieldType::TEXT, "c_zip", true);
    addField(schemaBuilder, FieldType::TEXT, "c_phone", true);
    addField(schemaBuilder, FieldType::BIGINT, "c_since", true);
    addField(schemaBuilder, FieldType::TEXT, "c_credit", true);
    addField(schemaBuilder, FieldType::BIGINT, "c_credit_lim", true);    //numeric (12,2)
    addField(schemaBuilder, FieldType::INT, "c_discount", true);         //numeric (4,4)
    addField(schemaBuilder, FieldType::BIGINT, "c_balance", true);       //numeric (12,2)
    addField(schemaBuilder, FieldType::BIGINT, "c_ytd_payment", true);   //numeric (12,2)
    addField(schemaBuilder, FieldType::SMALLINT, "c_payment_cnt", true);
    addField(schemaBuilder, FieldType::SMALLINT, "c_delivery_cnt", true);
    addField(schemaBuilder, FieldType::TEXT, "c_data", true);
    if (useCH)
        addField(schemaBuilder, FieldType::SMALLINT, "c_n_nationkey", true);
    schemaBuilder.SetPrimaryKey({"c_w_id", "c_d_id", "c_id"});
    {
        std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
        tableCreator->num_replicas(1);
        kudu::client::KuduSchemaBuilder schemaBuilder;

        addField(schemaBuilder, FieldType::SMALLINT, "c_w_id", true);
        addField(schemaBuilder, FieldType::SMALLINT, "c_d_id", true);
        addField(schemaBuilder, FieldType::TEXT, "c_last", true);
        addField(schemaBuilder, FieldType::TEXT, "c_first", true);
        addField(schemaBuilder, FieldType::INT, "c_id", true);
        schemaBuilder.SetPrimaryKey({"c_w_id", "c_d_id", "c_last", "c_first", "c_id"});

        kudu::client::KuduSchema schema;
        assertOk(schemaBuilder.Build(&schema));
        tableCreator->schema(&schema);
        tableCreator->table_name("c_last_idx");
        tableCreator->Create();
    }

    kudu::client::KuduSchema schema;
    assertOk(schemaBuilder.Build(&schema));
    tableCreator->schema(&schema);
    tableCreator->table_name("customer");
    tableCreator->Create();
}

void createHistory(KuduSession& session) {
    // this one has no primary key
    std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
    tableCreator->num_replicas(1);
    kudu::client::KuduSchemaBuilder schemaBuilder;

    addField(schemaBuilder, FieldType::INT, "h_c_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "h_c_d_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "h_c_w_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "h_d_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "h_w_id", true);
    addField(schemaBuilder, FieldType::BIGINT, "h_date", true);          //datetime (nanosecs since 1970)
    addField(schemaBuilder, FieldType::INT, "h_amount", true);           //numeric (6,2)
    addField(schemaBuilder, FieldType::TEXT, "h_data", true);

    kudu::client::KuduSchema schema;
    assertOk(schemaBuilder.Build(&schema));
    tableCreator->schema(&schema);
    tableCreator->table_name("history");
    tableCreator->Create();
}

void createNewOrder(KuduSession& session) {
    // Primary key: (no_w_id, no_d_id, no_o_id)
    //              (2 b    , 1 b    , 4 b    )
    std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
    tableCreator->num_replicas(1);
    kudu::client::KuduSchemaBuilder schemaBuilder;

    addField(schemaBuilder, FieldType::SMALLINT, "no_w_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "no_d_id", true);
    addField(schemaBuilder, FieldType::INT, "no_o_id", true);
    schemaBuilder.SetPrimaryKey({"no_w_id", "no_d_id", "no_o_id"});

    kudu::client::KuduSchema schema;
    assertOk(schemaBuilder.Build(&schema));
    tableCreator->schema(&schema);
    tableCreator->table_name("new-order");
    tableCreator->Create();
}

void createOrder(KuduSession& session) {
    // Primary key: (o_w_id, o_d_id, o_id)
    //              (2 b   , 1 b   , 4 b )
    std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
    tableCreator->num_replicas(1);
    kudu::client::KuduSchemaBuilder schemaBuilder;

    addField(schemaBuilder, FieldType::SMALLINT, "o_w_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "o_d_id", true);
    addField(schemaBuilder, FieldType::INT, "o_id", true);
    addField(schemaBuilder, FieldType::INT, "o_c_id", true);
    addField(schemaBuilder, FieldType::BIGINT, "o_entry_d", true);           //datetime (nanosecs since 1970)
    addField(schemaBuilder, FieldType::SMALLINT, "o_carrier_id", false);
    addField(schemaBuilder, FieldType::SMALLINT, "o_ol_cnt", true);
    addField(schemaBuilder, FieldType::SMALLINT, "o_all_local", true);
    schemaBuilder.SetPrimaryKey({"o_w_id", "o_d_id", "o_id"});

    kudu::client::KuduSchema schema;
    assertOk(schemaBuilder.Build(&schema));
    tableCreator->schema(&schema);
    tableCreator->table_name("order");
    tableCreator->Create();
    {
        std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
        tableCreator->num_replicas(1);
        kudu::client::KuduSchemaBuilder schemaBuilder;

        addField(schemaBuilder, FieldType::SMALLINT, "o_w_id", true);
        addField(schemaBuilder, FieldType::SMALLINT, "o_d_id", true);
        addField(schemaBuilder, FieldType::INT, "o_c_id", true);
        addField(schemaBuilder, FieldType::INT, "o_id", true);
        schemaBuilder.SetPrimaryKey({"o_w_id", "o_d_id", "o_c_id", "o_id"});

        kudu::client::KuduSchema schema;
        assertOk(schemaBuilder.Build(&schema));
        tableCreator->schema(&schema);
        tableCreator->table_name("order_idx");
        tableCreator->Create();
    }
}

void createOrderLine(KuduSession& session) {
    // Primary Key: (ol_w_id, ol_d_id, ol_o_id, ol_number)
    //              (2 b    , 1 b    , 4 b    , 1 b      )
    std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
    tableCreator->num_replicas(1);
    kudu::client::KuduSchemaBuilder schemaBuilder;

    addField(schemaBuilder, FieldType::SMALLINT, "ol_w_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "ol_d_id", true);
    addField(schemaBuilder, FieldType::INT, "ol_o_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "ol_number", true);
    addField(schemaBuilder, FieldType::INT, "ol_i_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "ol_supply_w_id", true);
    addField(schemaBuilder, FieldType::BIGINT, "ol_delivery_d", false);      //datetime (nanosecs since 1970)
    addField(schemaBuilder, FieldType::SMALLINT, "ol_quantity", true);
    addField(schemaBuilder, FieldType::INT, "ol_amount", true);              //numeric (6,2)
    addField(schemaBuilder, FieldType::TEXT, "ol_dist_info", true);
    schemaBuilder.SetPrimaryKey({"ol_w_id", "ol_d_id", "ol_o_id", "ol_number"});

    kudu::client::KuduSchema schema;
    assertOk(schemaBuilder.Build(&schema));
    tableCreator->schema(&schema);
    tableCreator->table_name("order-line");
    tableCreator->Create();
}

void createItem(KuduSession& session) {
    // Primary key: (i_id)
    //              (4 b )
    std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
    tableCreator->num_replicas(1);
    kudu::client::KuduSchemaBuilder schemaBuilder;

    addField(schemaBuilder, FieldType::INT, "i_id", true);
    addField(schemaBuilder, FieldType::INT, "i_im_id", true);
    addField(schemaBuilder, FieldType::TEXT, "i_name", true);
    addField(schemaBuilder, FieldType::INT, "i_price", true);        // numeric (5,2)
    addField(schemaBuilder, FieldType::TEXT, "i_data", true);
    schemaBuilder.SetPrimaryKey({"i_id"});

    kudu::client::KuduSchema schema;
    assertOk(schemaBuilder.Build(&schema));
    tableCreator->schema(&schema);
    tableCreator->table_name("item");
    tableCreator->Create();
}

void createStock(KuduSession& session, bool useCH) {
    // Primary key: (s_w_id, s_i_id)
    //              ( 2 b  , 4 b   )
    std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
    tableCreator->num_replicas(1);
    kudu::client::KuduSchemaBuilder schemaBuilder;

    addField(schemaBuilder, FieldType::SMALLINT, "s_w_id", true);
    addField(schemaBuilder, FieldType::INT, "s_i_id", true);
    addField(schemaBuilder, FieldType::INT, "s_quantity", true);
    addField(schemaBuilder, FieldType::TEXT, "s_dist_01", true);
    addField(schemaBuilder, FieldType::TEXT, "s_dist_02", true);
    addField(schemaBuilder, FieldType::TEXT, "s_dist_03", true);
    addField(schemaBuilder, FieldType::TEXT, "s_dist_04", true);
    addField(schemaBuilder, FieldType::TEXT, "s_dist_05", true);
    addField(schemaBuilder, FieldType::TEXT, "s_dist_06", true);
    addField(schemaBuilder, FieldType::TEXT, "s_dist_07", true);
    addField(schemaBuilder, FieldType::TEXT, "s_dist_08", true);
    addField(schemaBuilder, FieldType::TEXT, "s_dist_09", true);
    addField(schemaBuilder, FieldType::TEXT, "s_dist_10", true);
    addField(schemaBuilder, FieldType::INT, "s_ytd", true);
    addField(schemaBuilder, FieldType::SMALLINT, "s_order_cnt", true);
    addField(schemaBuilder, FieldType::SMALLINT, "s_remote_cnt", true);
    addField(schemaBuilder, FieldType::TEXT, "s_data", true);
    if (useCH)
        addField(schemaBuilder, FieldType::SMALLINT, "s_su_suppkey", true);
    schemaBuilder.SetPrimaryKey({"s_w_id", "s_i_id"});

    kudu::client::KuduSchema schema;
    assertOk(schemaBuilder.Build(&schema));
    tableCreator->schema(&schema);
    tableCreator->table_name("stock");
    tableCreator->Create();
}

void createRegion(KuduSession& session) {
    // Primary key: (r_regionkey)
    //              ( 2 b )
    std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
    tableCreator->num_replicas(1);
    kudu::client::KuduSchemaBuilder schemaBuilder;

    addField(schemaBuilder, FieldType::SMALLINT, "r_regionkey", true);
    addField(schemaBuilder, FieldType::TEXT, "r_name", true);
    addField(schemaBuilder, FieldType::TEXT, "r_comment", true);
    schemaBuilder.SetPrimaryKey({"r_regionkey"});

    kudu::client::KuduSchema schema;
    assertOk(schemaBuilder.Build(&schema));
    tableCreator->schema(&schema);
    tableCreator->table_name("region");
    tableCreator->Create();
}

void createNation(KuduSession& session) {
    // Primary key: (r_nationkey)
    //              ( 2 b )
    std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
    tableCreator->num_replicas(1);
    kudu::client::KuduSchemaBuilder schemaBuilder;

    addField(schemaBuilder, FieldType::SMALLINT, "n_nationkey", true);
    addField(schemaBuilder, FieldType::TEXT, "n_name", true);
    addField(schemaBuilder, FieldType::SMALLINT, "n_regionkey", true);
    addField(schemaBuilder, FieldType::TEXT, "n_comment", true);
    schemaBuilder.SetPrimaryKey({"n_nationkey"});

    kudu::client::KuduSchema schema;
    assertOk(schemaBuilder.Build(&schema));
    tableCreator->schema(&schema);
    tableCreator->table_name("nation");
    tableCreator->Create();
}

void createSupplier(KuduSession& session) {
    // Primary key: (su_suppkey)
    //              ( 2 b )
    std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
    tableCreator->num_replicas(1);
    kudu::client::KuduSchemaBuilder schemaBuilder;

    addField(schemaBuilder, FieldType::SMALLINT, "su_suppkey", true);
    addField(schemaBuilder, FieldType::TEXT, "su_name", true);
    addField(schemaBuilder, FieldType::TEXT, "su_address", true);
    addField(schemaBuilder, FieldType::SMALLINT, "su_nationkey", true);
    addField(schemaBuilder, FieldType::TEXT, "su_phone", true);
    addField(schemaBuilder, FieldType::BIGINT, "su_acctbal", true);  //numeric (12,2)
    addField(schemaBuilder, FieldType::TEXT, "su_comment", true);

    kudu::client::KuduSchema schema;
    assertOk(schemaBuilder.Build(&schema));
    tableCreator->schema(&schema);
    tableCreator->table_name("supplier");
    tableCreator->Create();
}

} // anonymous namespace

void createSchema(kudu::client::KuduSession& session, bool useCH) {
    createWarehouse(session);
    createDistrict(session);
    createCustomer(session, useCH);
    createHistory(session);
    createNewOrder(session);
    createOrder(session);
    createOrderLine(session);
    createItem(session);
    createStock(session, useCH);
    if (useCH) {
        createRegion(session);
        createNation(session);
        createSupplier(session);
    }
}

} // namespace tpcc
