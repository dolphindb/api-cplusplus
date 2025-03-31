// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#if __cplusplus < 201402L
#include <type_traits>
namespace std {
	template< bool B, class T = void >
	using enable_if_t = typename enable_if<B,T>::type;
}
#endif
