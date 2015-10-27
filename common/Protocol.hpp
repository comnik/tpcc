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

#include <crossbow/Serializer.hpp>
#include <crossbow/string.hpp>

namespace tpcc {

enum class Command {
    POPULATE_WAREHOUSE = 1,
    CREATE_SCHEMA
};

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

namespace impl {

template<class... Args>
struct SerializeHelper;

template<class Head, class... Tail>
struct SerializeHelper<Head, Tail...> {
    SerializeHelper<Tail...> mTail;
    void size(crossbow::sizer& sizer, const Head& head, const Tail&... tail) const {
        sizer & head;
        mTail.size(sizer, tail...);
    }
    void serialize(crossbow::serializer& ser, const Head& head, const Tail&... tail) const {
        ser & head;
        mTail.serialize(ser, tail...);
    }
};

template<>
struct SerializeHelper<> {
    void size(crossbow::sizer& sizer) const {
    }
    void serialize(crossbow::serializer& ser) const {
    }
};

template<size_t Idx, class Tuple>
struct TupleSerializer {
    TupleSerializer<Idx - 1, Tuple> mChild;
    void size(crossbow::sizer& sizer, const Tuple& tuple) const {
        sizer & std::get<Idx>(tuple);
        mChild.size(sizer, tuple);
    }

    void serialize(crossbow::serializer& ser, const Tuple& tuple) const {
        mChild.serialize(ser, tuple);
        ser & std::get<Idx>(tuple);
    }
};

template<class Tuple>
struct TupleSerializer<0, Tuple> {
    template<size_t Sz = std::tuple_size<Tuple>::value>
    typename std::enable_if<Sz != 0, void>::type size(crossbow::sizer& sizer, const Tuple& tuple) const {
        sizer & std::get<0>(tuple);
    }
    template<size_t Sz = std::tuple_size<Tuple>::value>
    typename std::enable_if<Sz == 0, void>::type size(crossbow::sizer& sizer, const Tuple& tuple) const {
    }

    template<size_t Sz = std::tuple_size<Tuple>::value>
    typename std::enable_if<Sz != 0, void>::type serialize(crossbow::serializer& ser, const Tuple& tuple) const {
        ser & std::get<0>(tuple);
    }
    template<size_t Sz = std::tuple_size<Tuple>::value>
    typename std::enable_if<Sz == 0, void>::type serialize(crossbow::serializer& ser, const Tuple& tuple) const {
    }
};

template<size_t Idx, class Tuple>
struct DeserializeHelper {
    DeserializeHelper<Idx - 1, Tuple> mChild;
    void deserialize(crossbow::deserializer& d, Tuple& t) const {
        mChild.deserialize(d, t);
        d & std::get<Idx>(t);
    }
};

template<>
struct DeserializeHelper<0, std::tuple<>> {
    void deserialize(crossbow::deserializer& d, std::tuple<>& t) const {
    }
};

template<class Tuple>
struct DeserializeHelper<static_cast<size_t>(0), Tuple> {

    template<size_t Sz = std::tuple_size<Tuple>::value>
    typename std::enable_if<Sz != 0, void>::type deserialize(crossbow::deserializer& d, Tuple& t) const {
        d & std::get<0>(t);
    }

    template<size_t Sz = std::tuple_size<Tuple>::value>
    typename std::enable_if<Sz == 0, void>::type deserialize(crossbow::deserializer& d, Tuple& t) const {
    }
};

}

namespace client {

class CommandsImpl {
    boost::asio::ip::tcp::socket& mSocket;
    std::unique_ptr<uint8_t[]> mCurrentRequest;
    size_t mCurrSize;
public:
    CommandsImpl(boost::asio::ip::tcp::socket& socket)
        : mSocket(socket), mCurrentRequest(new uint8_t[1024])
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
            impl::DeserializeHelper<std::tuple_size<Result>::value - 1, Result> des;
            des.deserialize(ser, res);
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
        impl::SerializeHelper<Args...> argSerializer;
        argSerializer.size(sizer, args...);
        if (mCurrSize < sizer.size) {
            mCurrentRequest.reset(new uint8_t[sizer.size]);
            mCurrSize = sizer.size;
        }
        crossbow::serializer ser(mCurrentRequest.get());
        argSerializer.serialize(ser, args...);
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
        constexpr auto firstIndex = std::tuple_size<Args>::value == 0 ? 0 : std::tuple_size<Args>::value - 1;
        impl::DeserializeHelper<firstIndex, Args> desHelper;
        desHelper.deserialize(des, args);
        mImpl.template execute<C>(args, [this](const Res& result) {
            // Serialize result
            constexpr auto firstIndex = std::tuple_size<Res>::value == 0 ? 0 : std::tuple_size<Res>::value - 1;
            impl::TupleSerializer<firstIndex, Res> tSer;
            crossbow::sizer sizer;
            tSer.size(sizer, result);
            if (mBufSize < sizer.size) {
                mBuffer.reset(new uint8_t[sizer.size]);
            }
            crossbow::serializer ser(mBuffer.get());
            tSer.serialize(ser, result);
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
        auto reqSize = *reinterpret_cast<size_t*>(mBuffer.get());
        if (bytes_read != 0 && reqSize == bytes_read) {
            // done reading
            auto cmd = *reinterpret_cast<Command*>(mBuffer.get() + sizeof(size_t));
            switch (cmd) {
            case Command::POPULATE_WAREHOUSE:
                execute<Command::POPULATE_WAREHOUSE>();
                break;
            case Command::CREATE_SCHEMA:
                execute<Command::CREATE_SCHEMA>();
            }
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

