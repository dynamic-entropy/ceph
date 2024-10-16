#include "rgw_redis_queue.h"

namespace rgw {
namespace redisqueue {

// FIXME: Perhaps return the queue length in calls to reserve, commit, abort
// etc and do not use this function explicitly?
int queue_status(connection* conn, const std::string& name,
                 std::tuple<int, int>& res, optional_yield y) {
  boost::redis::request req;
  boost::redis::response<int, int> resp;
  boost::system::error_code ec;

  try {
    req.push("LLEN", "reserve:" + name);
    req.push("LLEN", "queue:" + name);

    rgw::redis::redis_exec(conn, ec, req, resp, y);
    if (ec) {
      std::cerr << "RGW Redis Queue:: " << __func__
                << "(): ERROR: " << ec.message() << std::endl;
      return -ec.value();
    }
    res = std::make_tuple(std::get<0>(resp).value(), std::get<1>(resp).value());
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "RGW Redis Queue:: " << __func__
              << "(): Exception: " << e.what() << std::endl;
    return -EINVAL;
  }
}

int reserve(connection* conn, const std::string name, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  int reserveSize = 120;
  req.push("FCALL", "reserve", 1, name, reserveSize);
  return rgw::redis::do_redis_func(conn, req, resp, __func__, y).errorCode;
}

int commit(connection* conn, const std::string& name, const std::string& data,
           optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "commit", 1, name, data);
  return rgw::redis::do_redis_func(conn, req, resp, __func__, y).errorCode;
}

int abort(connection* conn, const std::string& name, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "abort", 1, name);
  return rgw::redis::do_redis_func(conn, req, resp, __func__, y).errorCode;
}

int read(connection* conn, const std::string& name, std::string& res,
         optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "read", 1, name);
  rgw::redis::RedisResponse ret =
      rgw::redis::do_redis_func(conn, req, resp, __func__, y);
  if (ret.errorCode == 0) {
    res = ret.data;
  } else {
    std::cerr << "RGW Redis Queue:: " << __func__
              << "(): ERROR: " << ret.errorMessage << std::endl;
    res = "";
  }

  return ret.errorCode;
}

int locked_read(connection* conn, const std::string& name,
                std::string& lock_cookie, std::string& res, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "locked_read", 1, name, lock_cookie);
  rgw::redis::RedisResponse ret =
      rgw::redis::do_redis_func(conn, req, resp, __func__, y);
  if (ret.errorCode == 0) {
    res = ret.data;
  } else {
    std::cerr << "RGW Redis Queue:: " << __func__
              << "(): ERROR: " << ret.errorMessage << std::endl;
    res = "";
  }

  return ret.errorCode;
}

int locked_read(connection* conn, const std::string& name,
                std::string& lock_cookie, std::vector<std::string>& res,
                int count, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "locked_read_multi", 1, name, lock_cookie, count);
  rgw::redis::RedisResponse ret =
      rgw::redis::do_redis_func(conn, req, resp, __func__, y);
  if (ret.errorCode == 0) {
    // Parse the json string
    boost::json::value v = boost::json::parse(ret.data);
    // Check if the json value is an array
    if (!v.is_array()) {
      std::cerr << "RGW Redis Queue:: " << __func__
                << "(): ERROR: JSON value is not an array" << std::endl;
      return -EINVAL;
    }

    // Iterate over the json array and add each element to the vector
    for (auto& element : v.as_array()) {
      res.push_back(element.as_string().c_str());
    }

  } else {
    std::cerr << "RGW Redis Queue:: " << __func__
              << "(): ERROR: " << ret.errorMessage << std::endl;
  }

  return ret.errorCode;
}

int ack(connection* conn, const std::string& name, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "ack", 1, name);
  return rgw::redis::do_redis_func(conn, req, resp, __func__, y).errorCode;
}

int locked_ack(connection* conn, const std::string& name,
               const std::string& lock_cookie, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "locked_ack", 1, name, lock_cookie);
  return rgw::redis::do_redis_func(conn, req, resp, __func__, y).errorCode;
}

int locked_ack(connection* conn, const std::string& name,
               const std::string& lock_cookie, int count, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "locked_ack_multi", 1, name, lock_cookie, count);
  return rgw::redis::do_redis_func(conn, req, resp, __func__, y).errorCode;
}

int cleanup_stale_reservations(connection* conn, const std::string& name,
                               int stale_timeout, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "cleanup", 1, name, stale_timeout);
  return rgw::redis::do_redis_func(conn, req, resp, __func__, y).errorCode;
}

}  // namespace redisqueue
}  // namespace rgw