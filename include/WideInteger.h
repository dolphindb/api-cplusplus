///
/// ref: https://github.com/abseil/abseil-cpp/blob/master/absl/numeric/int128.h
///
#pragma once
#include <functional>
#include <limits>
#include <iosfwd>
#include <type_traits>

#include <stdint.h>
#include "absl/numeric/int128.h"

namespace wide_integer {

using int128 = absl::int128;
using uint128 = absl::uint128;

template <class T>
inline void hash_combine(std::size_t &seed, const T &v) {
    std::hash<T> hasher;
    seed ^= hasher(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
}

// Specialized std::hash for uint128 and int128.

}


namespace std {
template <>
struct hash<wide_integer::int128> {
    size_t operator()(wide_integer::int128 v) const noexcept {
        size_t seed = 0;
        wide_integer::hash_combine(seed, absl::Int128High64(v));
        wide_integer::hash_combine(seed, absl::Int128Low64(v));
        return seed;
    }
};

template <>
struct hash<wide_integer::uint128> {
    size_t operator()(wide_integer::uint128 v) const noexcept {
        size_t seed = 0;
        wide_integer::hash_combine(seed, absl::Uint128High64(v));
        wide_integer::hash_combine(seed, absl::Uint128Low64(v));
        return seed;
    }
};

template<>
struct make_unsigned<wide_integer::int128>{
    using type = wide_integer::uint128;
};
}  // namespace std

