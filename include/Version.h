// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include <algorithm>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace dolphindb {

// Please use DBConnection::checkVersion
struct VersionT {
    VersionT(int majorVersion = 0, int minorVersion = 0, int patchVersion = 0)
    {
        major = majorVersion;
        minor = minorVersion;
        patch = patchVersion;
    }

    int major;
    int minor;
    int patch;

    static bool parse(const std::string &version, VersionT &result)
    {
        std::istringstream iss(version);
        VersionT parsed;
        int build;
        char dot;

        if (!(iss >> parsed.major >> dot) || dot != '.' ||
            !(iss >> build >> dot) || dot != '.' ||
            !(iss >> parsed.minor)) {
            return false;
        }

        int const next = iss.peek();
        if (next == '.') {
            iss.get();
            if (!(iss >> parsed.patch)) {
                return false;
            }
        }

        constexpr int MAX_VERSION{100};
        parsed.minor += build * MAX_VERSION;
        result = parsed;
        return true;
    }

    bool check(const std::vector<VersionT> &required)
    {
        int max_major{0};
        for (const auto &r : required) {
            max_major = std::max(r.major, max_major);
            if (r.major != major) {
                continue;
            }
            return std::make_pair(minor, patch) >= std::make_pair(r.minor, r.patch);
        }
        return major > max_major;
    }
};

} // namespace dolphindb
