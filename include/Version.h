// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include <vector>

namespace dolphindb {

// Please use DBConnection::checkVersion
struct VersionT {
    int major;
    int minor;
    int patch;
    bool check(const std::vector<VersionT> &required)
    {
        int max_major{0};
        for (auto &r : required) {
            max_major = std::max(r.major, max_major);
            if (r.major != major) {
                continue;
            }
            return std::make_pair(minor, patch) >= std::make_pair(r.minor, r.patch);
        }
        return major > max_major;
    }
};

}
