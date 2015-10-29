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
#include <tuple>
#include <cstdint>
#include <type_traits>

#include <boost/system/error_code.hpp>
#include <boost/asio.hpp>
#include <boost/preprocessor.hpp>

#include <crossbow/Serializer.hpp>
#include <crossbow/string.hpp>

#define GEN_COMMANDS_ARR(Name, arr) enum class Name {\
    BOOST_PP_ARRAY_ELEM(0, arr) = 1, \
    BOOST_PP_ARRAY_ENUM(BOOST_PP_ARRAY_REMOVE(arr, 0)) \
} 

#define GEN_COMMANDS(Name, TUPLE) GEN_COMMANDS_ARR(Name, (BOOST_PP_TUPLE_SIZE(TUPLE), TUPLE))

#define EXPAND_CASE(r, t) case BOOST_PP_TUPLE_ELEM(2, 0, t)::BOOST_PP_ARRAY_ELEM(0, BOOST_PP_TUPLE_ELEM(2, 1, t)): \
    execute<BOOST_PP_TUPLE_ELEM(2, 0, t)::BOOST_PP_ARRAY_ELEM(0, BOOST_PP_TUPLE_ELEM(2, 1, t))>();\
    break;

#define SWITCH_PREDICATE(r, state) BOOST_PP_ARRAY_SIZE(BOOST_PP_TUPLE_ELEM(2, 1, state))

#define SWITCH_REMOVE_ELEM(r, state) (\
        BOOST_PP_TUPLE_ELEM(2, 0, state), \
        BOOST_PP_ARRAY_REMOVE(BOOST_PP_TUPLE_ELEM(2, 1, state), 0) \
        )\

#define SWITCH_CASE_IMPL(Name, Param, arr) switch (Param) {\
    BOOST_PP_FOR((Name, arr), SWITCH_PREDICATE, SWITCH_REMOVE_ELEM, EXPAND_CASE) \
}

#define SWITCH_CASE(Name, Param, t) SWITCH_CASE_IMPL(Name, Param, (BOOST_PP_TUPLE_SIZE(t), t))

namespace tpcc {

#define COMMANDS (POPULATE_WAREHOUSE, CREATE_SCHEMA, NEW_ORDER, PAYMENT, ORDER_STATUS, DELIVERY, STOCK_LEVEL)

GEN_COMMANDS(Command, COMMANDS);

//enum class Command {
//    POPULATE_WAREHOUSE = 1,
//    CREATE_SCHEMA,
//    NEW_ORDER,
//    PAYMENT
//};

template<Command C>
struct Signature;

template<>
struct Signature<Command::POPULATE_WAREHOUSE> {
    using result = std::tuple<bool, crossbow::string>;
    using arguments = std::tuple<int16_t>;
};

template<>
struct Signature<Command::CREATE_SCHEMA> {
    using result = std::tuple<bool, crossbow::string>;
    using arguments = std::tuple<>;
};

struct NewOrderIn {
    int16_t w_id;
    int16_t d_id;
    int16_t c_id;
};

struct NewOrderResult {
    using is_serializable = crossbow::is_serializable;
    struct OrderLine {
        int16_t ol_supply_w_id;
        int32_t ol_i_id;
        crossbow::string i_name;
        int16_t ol_quantity;
        int32_t s_quantity;
        char brand_generic;
        int32_t i_price;
        int32_t ol_amount;

        template<class Archiver>
        void operator&(Archiver& ar) {
            ar & ol_supply_w_id;
            ar & ol_i_id;
            ar & i_name;
            ar & ol_quantity;
            ar & s_quantity;
            ar & brand_generic;
            ar & i_price;
            ar & ol_amount;
        }
    };
    bool success = true;
    crossbow::string error;
    int32_t o_id;
    int16_t o_ol_cnt;
    crossbow::string c_last;
    int16_t c_credit;
    int32_t c_discount;
    int32_t w_tax;
    int32_t d_tax;
    int64_t o_entry_d;
    int32_t total_amount;
    std::vector<OrderLine> lines;

    template<class Archiver>
    void operator&(Archiver& ar) const {
        ar & success = true;
        ar & error;
        ar & o_id;
        ar & o_ol_cnt;
        ar & c_last;
        ar & c_credit;
        ar & c_discount;
        ar & w_tax;
        ar & d_tax;
        ar & o_entry_d;
        ar & total_amount;
        ar & lines;
    }
};

template<>
struct Signature<Command::NEW_ORDER> {
    using result = NewOrderResult;
    using arguments = NewOrderIn;
};

struct PaymentIn {
    using is_serializable = crossbow::is_serializable;
    bool selectByLastName;
    int16_t w_id;
    int16_t d_id;
    int16_t c_id;
    int16_t c_w_id;
    int16_t c_d_id;
    crossbow::string c_last;
    int32_t h_amount;

    template<class Archiver>
    void operator&(Archiver& ar) {
        ar & selectByLastName;
        ar & w_id;
        ar & d_id;
        ar & c_id;
        ar & c_w_id;
        ar & c_d_id;
        ar & c_last;
        ar & h_amount;
    }
};

struct PaymentResult {
    using is_serializable = crossbow::is_serializable;
    bool success = true;
    crossbow::string error;

    template<class Archiver>
    void operator&(Archiver& ar) {
        ar & success;
        ar & error;
    }
};

template<>
struct Signature<Command::PAYMENT> {
    using result = PaymentResult;
    using arguments = PaymentIn;
};

struct OrderStatusIn {
    using is_serializable = crossbow::is_serializable;
    bool selectByLastName;
    int16_t w_id;
    int16_t d_id;
    int16_t c_id;
    crossbow::string c_last;

    template<class A>
    void operator&(A& ar) {
        ar & selectByLastName;
        ar & w_id;
        ar & d_id;
        ar & c_id;
        ar & c_last;
    }
};

struct OrderStatusResult {
    using is_serializable = crossbow::is_serializable;
    bool success;
    crossbow::string error;

    template<class A>
    void operator&(A& ar) {
        ar & success;
        ar & error;
    }
};

template<>
struct Signature<Command::ORDER_STATUS> {
    using arguments = OrderStatusIn;
    using result = OrderStatusResult;
};

struct DeliveryIn {
    using is_serializable = crossbow::is_serializable;
    int16_t w_id;
    int16_t o_carrier_id;

    template<class A>
    void operator&(A& ar) {
        ar & w_id;
        ar & o_carrier_id;
    }
};

struct DeliveryResult {
    using is_serializable = crossbow::is_serializable;
    bool success;
    crossbow::string error;

    template<class A>
    void operator& (A& ar) {
        ar & success;
        ar & error;
    }
};

template<>
struct Signature<Command::DELIVERY> {
    using arguments = DeliveryIn;
    using result = DeliveryResult;
};

struct StockLevelIn {
};

struct StockLevelResult {
    using is_serializable = crossbow::is_serializable;
    bool success;
    crossbow::string error;

    template<class A>
    void operator& (A& ar) {
        ar & success;
        ar & error;
    }
};

template<>
struct Signature<Command::STOCK_LEVEL> {
    using arguments = StockLevelIn;
    using result = StockLevelResult;
};

namespace impl {

template<class... Args>
struct ArgSerializer;

template<class Head, class... Tail>
struct ArgSerializer<Head, Tail...> {
    ArgSerializer<Tail...> rest;

    template<class C>
    void exec(C& c, const Head& head, const Tail&... tail) const {
        c & head;
        rest.exec(c, tail...);
    }
};

template<>
struct ArgSerializer<> {
    template<class C>
    void exec(C&) const {}
};

}

namespace client {

class CommandsImpl {
    boost::asio::ip::tcp::socket& mSocket;
    size_t mCurrSize = 1024;
    std::unique_ptr<uint8_t[]> mCurrentRequest;
public:
    CommandsImpl(boost::asio::ip::tcp::socket& socket)
        : mSocket(socket), mCurrentRequest(new uint8_t[mCurrSize])
    {
    }
    template<class Callback, class Result>
    void readResonse(const Callback& callback, size_t bytes_read = 0) {
        auto respSize = *reinterpret_cast<size_t*>(mCurrentRequest.get());
        if (bytes_read >= 8 && respSize == bytes_read) {
            // response read
            Result res;
            boost::system::error_code noError;
            crossbow::deserializer ser(mCurrentRequest.get() + sizeof(size_t));
            ser & res;
            callback(noError, res);
            return;
        } else if (bytes_read >= 8 && respSize > mCurrSize) {
            std::unique_ptr<uint8_t[]> newBuf(new uint8_t[respSize]);
            memcpy(newBuf.get(), mCurrentRequest.get(), mCurrSize);
            mCurrentRequest.swap(newBuf);
        }
        mSocket.async_read_some(boost::asio::buffer(mCurrentRequest.get() + bytes_read, mCurrSize - bytes_read),
                [this, callback, bytes_read](const boost::system::error_code& ec, size_t br){
                    if (ec) {
                        Result res;
                        callback(ec, res);
                    }
                    readResonse<Callback, Result>(callback, bytes_read + br);
                });
    }

    template<Command C, class Callback, class... Args>
    void execute(const Callback& callback, const Args&... args) {
        static_assert(std::is_same<typename Signature<C>::arguments, std::tuple<Args...>>::value,
                "Wrong function arguments");
        using ResType = typename Signature<C>::result;
        crossbow::sizer sizer;
        sizer & sizer.size;
        sizer & C;
        impl::ArgSerializer<Args...> argSerializer;
        argSerializer.exec(sizer, args...);
        if (mCurrSize < sizer.size) {
            mCurrentRequest.reset(new uint8_t[sizer.size]);
            mCurrSize = sizer.size;
        }
        crossbow::serializer ser(mCurrentRequest.get());
        ser & sizer.size;
        ser & C;
        argSerializer.exec(ser, args...);
        ser.buffer.release();
        boost::asio::async_write(mSocket, boost::asio::buffer(mCurrentRequest.get(), sizer.size),
                    [this, callback](const boost::system::error_code& ec, size_t){
                        if (ec) {
                            ResType res;
                            callback(ec, res);
                            return;
                        }
                        readResonse<Callback, ResType>(callback);
                    });
    }

};

} // namespace client

namespace server {

template<class Implementation>
class Server {
    Implementation& mImpl;
    boost::asio::ip::tcp::socket& mSocket;
    size_t mBufSize = 1024;
    std::unique_ptr<uint8_t[]> mBuffer;
    using error_code = boost::system::error_code;
public:
    Server(Implementation& impl, boost::asio::ip::tcp::socket& socket)
        : mImpl(impl)
        , mSocket(socket)
        , mBuffer(new uint8_t[mBufSize])
    {}
    void run() {
        read();
    }
private:
    template<Command C>
    void execute() {
        using Args = typename Signature<C>::arguments;
        using Res = typename Signature<C>::result;
        Args args;
        crossbow::deserializer des(mBuffer.get() + sizeof(size_t) + sizeof(Command));
        des & args;
        mImpl.template execute<C>(args, [this](const Res& result) {
            // Serialize result
            crossbow::sizer sizer;
            sizer & result;
            if (mBufSize < sizer.size) {
                mBuffer.reset(new uint8_t[sizer.size]);
            }
            crossbow::serializer ser(mBuffer.get());
            ser & result;
            ser.buffer.release();
            // send the result back
            boost::asio::async_write(mSocket,
                    boost::asio::buffer(mBuffer.get(), sizer.size),
                    [this](const error_code& ec, size_t bytes_written) {
                        if (ec) {
                            std::cerr << ec.message() << std::endl;
                            return;
                        }
                        read(0);
                    }
            );
        });
    }
    void read(size_t bytes_read = 0) {
        size_t reqSize = 0;
        if (bytes_read != 0) {
            reqSize = *reinterpret_cast<size_t*>(mBuffer.get());
        }
        if (bytes_read != 0 && reqSize == bytes_read) {
            // done reading
            auto cmd = *reinterpret_cast<Command*>(mBuffer.get() + sizeof(size_t));
            SWITCH_CASE(Command, cmd, COMMANDS)
            return;
        } else if (bytes_read >= 8 && reqSize > mBufSize) {
            std::unique_ptr<uint8_t[]> newBuf(new uint8_t[reqSize]);
            memcpy(newBuf.get(), mBuffer.get(), mBufSize);
            mBuffer.swap(newBuf);
        }
        mSocket.async_read_some(boost::asio::buffer(mBuffer.get() + bytes_read, mBufSize - bytes_read),
                [this, bytes_read](const error_code& ec, size_t br){
                    read(bytes_read + br);
                });
    }
};

} // namespace server

} // namespace tpcc

