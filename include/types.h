#pragma once
#include <tuple>

typedef std::tuple<int64_t, int64_t, int64_t> relnodeCoord;

relnodeCoord getRelnodeCoordinate(const std::string &fileName);