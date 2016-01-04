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

namespace tpcc {

using namespace kudu;
using namespace kudu::client;

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
    return result;
}

} // namespace tpcc
