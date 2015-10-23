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
#include "CreateSchema.hpp"
#include <telldb/Transaction.hpp>

namespace tpcc {

using namespace tell;

namespace {

void createWarehouse(db::Transaction& transaction) {
    // Warehouse table
    // w_id is used as primary key (since we are not issuing range queries)
    // w_id is a 16 bit number
    store::Schema schema(store::TableType::TRANSACTIONAL);
    schema.addField(store::FieldType::SMALLINT, "w_id", true);
    schema.addField(store::FieldType::TEXT, "w_name", true);
    schema.addField(store::FieldType::TEXT, "w_street_1", true);
    schema.addField(store::FieldType::TEXT, "w_street_2", true);
    schema.addField(store::FieldType::TEXT, "w_city", true);
    schema.addField(store::FieldType::TEXT, "w_state", true);
    schema.addField(store::FieldType::TEXT, "w_zip", true);
    schema.addField(store::FieldType::INT, "w_tax", true);
    schema.addField(store::FieldType::BIGINT, "w_ytd", true);
    transaction.createTable("warehouse", schema);
}

void createDistrict(db::Transaction& transaction) {
    store::Schema schema(store::TableType::TRANSACTIONAL);
    // Primary key: (d_w_id, d_id)
    //              ( 2 b    1 byte
    schema.addField(store::FieldType::SMALLINT, "d_id", true);
    schema.addField(store::FieldType::SMALLINT, "d_w_id", true);
    schema.addField(store::FieldType::TEXT, "d_name", true);
    schema.addField(store::FieldType::TEXT, "d_street_1", true);
    schema.addField(store::FieldType::TEXT, "d_street_2", true);
    schema.addField(store::FieldType::TEXT, "d_city", true);
    schema.addField(store::FieldType::TEXT, "d_state", true);
    schema.addField(store::FieldType::TEXT, "d_zip", true);
    schema.addField(store::FieldType::INT, "d_tax", true);
    schema.addField(store::FieldType::BIGINT, "d_ytd", true);
    schema.addField(store::FieldType::INT, "d_next_o_id", true);
    transaction.createTable("district", schema);
}

void createCustomer(db::Transaction& transaction) {
    // Primary key: (c_w_id, c_d_id, c_id)
    // c_w_id: 2 bytes
    // c_d_id: 1 byte
    // c_id: 2 bytes
    store::Schema schema(store::TableType::TRANSACTIONAL);
    schema.addField(store::FieldType::SMALLINT, "c_id", true);
    schema.addField(store::FieldType::SMALLINT, "c_d_id", true);
    schema.addField(store::FieldType::SMALLINT, "c_w_id", true);
    schema.addField(store::FieldType::TEXT, "c_first", true);
    schema.addField(store::FieldType::TEXT, "c_middle", true);
    schema.addField(store::FieldType::TEXT, "c_last", true);
    schema.addField(store::FieldType::TEXT, "c_street_1", true);
    schema.addField(store::FieldType::TEXT, "c_street_2", true);
    schema.addField(store::FieldType::TEXT, "c_city", true);
    schema.addField(store::FieldType::TEXT, "c_state", true);
    schema.addField(store::FieldType::TEXT, "c_zip", true);
    schema.addField(store::FieldType::TEXT, "c_phone", true);
    schema.addField(store::FieldType::BIGINT, "c_since", true);
    schema.addField(store::FieldType::SMALLINT, "c_credit", true);
    schema.addField(store::FieldType::BIGINT, "c_credit_lim", true);
    schema.addField(store::FieldType::INT, "c_discount", true);
    schema.addField(store::FieldType::BIGINT, "c_balance", true);
    schema.addField(store::FieldType::BIGINT, "c_ytd_payment", true);
    schema.addField(store::FieldType::SMALLINT, "c_payment_cnt", true);
    schema.addField(store::FieldType::SMALLINT, "c_delivery_cnt", true);
    schema.addField(store::FieldType::TEXT, "c_data", true);
    transaction.createTable("customer", schema);
}

void createHistory(db::Transaction& transaction) {
    // this one has no primary key
    store::Schema schema(store::TableType::TRANSACTIONAL);
    schema.addField(store::FieldType::INT, "h_c_id", true);
    schema.addField(store::FieldType::SMALLINT, "h_c_d_id", true);
    schema.addField(store::FieldType::SMALLINT, "h_c_w_id", true);
    schema.addField(store::FieldType::SMALLINT, "h_d_id", true);
    schema.addField(store::FieldType::SMALLINT, "h_w_id", true);
    schema.addField(store::FieldType::BIGINT, "h_date", true);
    schema.addField(store::FieldType::INT, "h_amount", true);
    schema.addField(store::FieldType::TEXT, "h_data", true);
    transaction.createTable("history", schema);
}

void createNewOrder(db::Transaction& transaction) {
    // Primary key: (no_w_id, no_d_id, no_o_id)
    //              (2 b    , 1 b    , 4 b    )
    store::Schema schema(store::TableType::TRANSACTIONAL);
    schema.addField(store::FieldType::INT, "no_o_id", true);
    schema.addField(store::FieldType::SMALLINT, "no_d_id", true);
    schema.addField(store::FieldType::SMALLINT, "no_w_id", true);
    transaction.createTable("new-order", schema);
}

void createOrder(db::Transaction& transaction) {
    // Primary key: (o_w_id, o_d_id, o_id)
    //              (2 b   , 1 b   , 4 b )
    store::Schema schema(store::TableType::TRANSACTIONAL);
    schema.addField(store::FieldType::INT, "o_id", true);
    schema.addField(store::FieldType::SMALLINT, "o_d_id", true);
    schema.addField(store::FieldType::SMALLINT, "o_w_id", true);
    schema.addField(store::FieldType::INT, "o_c_id", true);
    schema.addField(store::FieldType::BIGINT, "o_entry_d", true);
    schema.addField(store::FieldType::SMALLINT, "o_carrier_id", false);
    schema.addField(store::FieldType::SMALLINT, "o_ol_cnt", true);
    schema.addField(store::FieldType::SMALLINT, "o_all_local", true);
    transaction.createTable("order", schema);
}

void createOrderLine(db::Transaction& transaction) {
    // Primary Key: (ol_w_id, ol_d_id, ol_o_id, ol_number)
    //              (2 b    , 1 b    , 4 b    , 1 b      )
    store::Schema schema(store::TableType::TRANSACTIONAL);
    schema.addField(store::FieldType::INT, "ol_o_id", true);
    schema.addField(store::FieldType::SMALLINT, "ol_d_id", true);
    schema.addField(store::FieldType::SMALLINT, "ol_w_id", true);
    schema.addField(store::FieldType::SMALLINT, "ol_number", true);
    schema.addField(store::FieldType::INT, "ol_i_id", true);
    schema.addField(store::FieldType::SMALLINT, "ol_supply_w_id", true);
    schema.addField(store::FieldType::BIGINT, "ol_delivery_d", false);
    schema.addField(store::FieldType::SMALLINT, "ol_quantity", true);
    schema.addField(store::FieldType::INT, "ol_amount", true);
    schema.addField(store::FieldType::TEXT, "ol_dist_info", true);
    transaction.createTable("order-line", schema);
}

void createItem(db::Transaction& transaction) {
    // Primary key: (i_id)
    //              (4 b )
    store::Schema schema(store::TableType::TRANSACTIONAL);
    schema.addField(store::FieldType::INT, "i_id", true);
    schema.addField(store::FieldType::INT, "i_im_id", true);
    schema.addField(store::FieldType::TEXT, "i_name", true);
    schema.addField(store::FieldType::INT, "i_price", true);
    schema.addField(store::FieldType::TEXT, "i_data", true);
    transaction.createTable("item", schema);
}

void createStock(db::Transaction& transaction) {
    // Primary key: (s_w_id, s_i_id)
    //              ( 2 b  , 4 b   )
    store::Schema schema(store::TableType::TRANSACTIONAL);
    schema.addField(store::FieldType::INT, "s_i_id", true);
    schema.addField(store::FieldType::SMALLINT, "s_w_id", true);
    schema.addField(store::FieldType::INT, "s_quantity", true);
    schema.addField(store::FieldType::TEXT, "s_dist_01", true);
    schema.addField(store::FieldType::TEXT, "s_dist_02", true);
    schema.addField(store::FieldType::TEXT, "s_dist_03", true);
    schema.addField(store::FieldType::TEXT, "s_dist_04", true);
    schema.addField(store::FieldType::TEXT, "s_dist_05", true);
    schema.addField(store::FieldType::TEXT, "s_dist_06", true);
    schema.addField(store::FieldType::TEXT, "s_dist_07", true);
    schema.addField(store::FieldType::TEXT, "s_dist_08", true);
    schema.addField(store::FieldType::TEXT, "s_dist_09", true);
    schema.addField(store::FieldType::TEXT, "s_dist_10", true);
    schema.addField(store::FieldType::INT, "s_ytd", true);
    schema.addField(store::FieldType::SMALLINT, "s_order_cnt", true);
    schema.addField(store::FieldType::SMALLINT, "s_remote_cnt", true);
    schema.addField(store::FieldType::TEXT, "s_data", true);
    transaction.createTable("stock", schema);
}

} // anonymouse namespace

void createSchema(tell::db::Transaction& transaction) {
    createWarehouse(transaction);
    createDistrict(transaction);
    createCustomer(transaction);
    createHistory(transaction);
    createNewOrder(transaction);
    createOrder(transaction);
    createOrderLine(transaction);
    createItem(transaction);
    createStock(transaction);
    transaction.commit();
}

} // namespace tpcc
