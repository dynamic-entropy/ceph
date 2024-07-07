#pragma once

#include "rgw_redis_common.h"

namespace rgw {
namespace redisqueue {

using boost::redis::config;
using boost::redis::connection;

int initQueue(boost::asio::io_context& io, connection* conn, config* cfg,
              optional_yield y);

int reserve(connection* conn, const std::string name, optional_yield y);
int commit(connection* conn, const std::string& name, const std::string& data,
           optional_yield y);
int abort(connection* conn, const std::string& name, optional_yield y);
int read(connection* conn, const std::string& name, std::string& res,
         optional_yield y);

}  // namespace redisqueue
}  // namespace rgw