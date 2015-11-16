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
#include "Util.hpp"

namespace crossbow {

template class singleton<tpcc::Random_t, create_static<tpcc::Random_t>, default_lifetime<tpcc::Random_t>>;

} // namespace crossbow

namespace tpcc {

// splitting strings
std::vector<std::string> split(const std::string& str, const char delim) {
    std::stringstream ss(str);
    std::string item;
    std::vector<std::string> result;
    while (std::getline(ss, item, delim)) {
        if (item.empty()) continue;
        result.push_back(std::move(item));
    }
    return result;
}

Random_t::Random_t() {}

namespace {
uint32_t powerOf(uint32_t a, uint32_t x) {
    if (x == 0) return 1;
    if (x % 2 == 0) {
        auto tmp = powerOf(a, x / 2);
        return tmp*tmp;
    }
    return a*powerOf(a, x-1);
}

}

crossbow::string Random_t::nstring(unsigned x, unsigned y) {
    std::uniform_int_distribution<uint32_t> lenDist(x, y);
    auto len = lenDist(mRandomDevice);
    std::uniform_int_distribution<uint32_t> dist(0, powerOf(10, len) - 1);
    auto resNum = dist(mRandomDevice);
    crossbow::string res;
    res.resize(len);
    for (decltype(len) i = 0; i < len; ++i) {
        switch (resNum % 10) {
        case 0:
            res[len - i - 1] = '0';
            break;
        case 1:
            res[len - i - 1] = '1';
            break;
        case 2:
            res[len - i - 1] = '2';
            break;
        case 3:
            res[len - i - 1] = '3';
            break;
        case 4:
            res[len - i - 1] = '4';
            break;
        case 5:
            res[len - i - 1] = '5';
            break;
        case 6:
            res[len - i - 1] = '6';
            break;
        case 7:
            res[len - i - 1] = '7';
            break;
        case 8:
            res[len - i - 1] = '8';
            break;
        case 9:
            res[len - i - 1] = '9';
            break;
        }
        resNum /= 10;
    }
    return res;
}

crossbow::string Random_t::zipCode() {
    crossbow::string res;
    res.reserve(9);
    res.append(nstring(4, 4));
    res.append("11111");
    return res;
}

crossbow::string Random_t::astring(int x, int y) {
    std::uniform_int_distribution<int> lenDist(x, y);
    std::uniform_int_distribution<int> charDist(0, 255);
    auto length = lenDist(mRandomDevice);
    crossbow::string result;
    result.reserve(length);
    for (decltype(length) i = 0; i < length; ++i) {
        int charPos = charDist(mRandomDevice);
        if (charPos < 95) {
            result.push_back(char(0x21 + charPos)); // a printable ASCII/UTF-8 character
            continue;
        }
        constexpr uint16_t lowest6 = 0x3f;
        uint16_t unicodeValue = 0xc0 + (charPos - 95); // a printable unicode character
        // convert this to UTF-8
        uint8_t utf8[2] = {0xc0, 0x80}; // The UTF-8 base for 2-byte Characters
        // for the first char, we have to take the lowest 6 bit
        utf8[1] |= uint8_t(unicodeValue & lowest6);
        utf8[0] |= uint8_t(unicodeValue >> 6); // get the remaining bits
        assert((utf8[0] >> 5) == uint8_t(0x06)); // higher order byte starts with 110
        assert((utf8[1] >> 6) == uint8_t(0x2)); // lower order byte starts with 10
        result.push_back(*reinterpret_cast<char*>(utf8));
        result.push_back(*reinterpret_cast<char*>(utf8 + 1));
    }
    return result;
}

crossbow::string Random_t::cLastName(int rNum) {
    crossbow::string res;
    res.reserve(15);
    std::array<crossbow::string, 10> subStrings{{"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"}};
    for (int i = 0; i < 3; ++i) {
        res.append(subStrings[rNum % 10]);
        rNum /= 10;
    }
    return res;
}


int64_t now() {
    auto now = std::chrono::system_clock::now();
    return now.time_since_epoch().count();
}

}

