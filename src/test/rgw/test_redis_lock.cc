#include <gtest/gtest.h>
#include <rgw_redis_lock.h>

#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <fstream>
#include <iostream>
#include <string>
#include <memory>

#include "common/async/blocked_completion.h"

using boost::redis::config;
using boost::redis::connection;

class RGWRedisLockTest : public ::testing::Test {
 protected:
  boost::asio::io_context io;
  std::unique_ptr<connection> conn;
  std::unique_ptr<config> cfg;

  void SetUp() {
    cfg = std::make_unique<config>();
    conn = std::make_unique<connection>(io);

    boost::asio::spawn(
        io,
        [this](boost::asio::yield_context yield) {
          int res = rgw::redis::load_lua_rgwlib(io, conn, cfg, yield);
          ASSERT_EQ(res, 0);
        },
        [this](std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
          io.stop();
        });
    io.run();
  }

  void TearDown() {}
};

TEST_F(RGWRedisLockTest, Lock) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int duration = 1000;
        const std::string name = "lock:lock";
        const std::string cookie = "mycookie";

        int return_code =
            rgw::redislock::lock(conn, name, cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        return_code = rgw::redislock::assert_locked(conn, name, cookie, yield);
        ASSERT_EQ(return_code, 0);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisLockTest, Unlock) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        const std::string name = "lock:unlock";
        const std::string cookie = "mycookie";
        int duration = 1000;

        int return_code =
            rgw::redislock::lock(conn, name, cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        return_code = rgw::redislock::unlock(conn, name, cookie, yield);
        ASSERT_EQ(return_code, 0);

        return_code = rgw::redislock::assert_locked(conn, name, cookie, yield);
        ASSERT_EQ(return_code, -ENOENT);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisLockTest, RenewBeforeLeaseExpiry) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        const std::string name = "lock:renew";
        const std::string cookie = "mycookie";
        int duration = 1000;

        int return_code =
            rgw::redislock::lock(conn, name, cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        // wait for 500ms
        boost::asio::steady_timer timer(io, std::chrono::milliseconds(500));
        timer.async_wait(yield);

        return_code = rgw::redislock::lock(conn, name, cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        // wait for 600ms - the initial lock timeout has expired by now but the
        // renewel process has kept it valid
        return_code = rgw::redislock::assert_locked(conn, name, cookie, yield);
        ASSERT_EQ(return_code, 0);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
}

// Lock is expired and then taken over by another client
// A renew attempt shall fail with EBUSY
TEST_F(RGWRedisLockTest, RenewAfterReacquisition) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        const std::string name = "lock:renew";
        const std::string cookie = "mycookie";
        int duration = 500;

        int return_code =
            rgw::redislock::lock(conn, name, cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        return_code = rgw::redislock::assert_locked(conn, name, cookie, yield);
        ASSERT_EQ(return_code, 0);

        // wait for the lock to expire
        boost::asio::steady_timer timer(io, std::chrono::milliseconds(1000));
        timer.async_wait(yield);

        const std::string new_cookie = "differentcookie";
        return_code =
            rgw::redislock::lock(conn, name, new_cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        // attempt to lock with the initial client
        return_code = rgw::redislock::lock(conn, name, cookie, duration, yield);
        ASSERT_EQ(return_code, -EBUSY);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisLockTest, MultiLock) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int duration = 12000;
        const std::string name = "lock:multi";
        const std::string clientCookie1 = "mycookie1";
        const std::string clientCookie2 = "mycookie2";

        int return_code =
            rgw::redislock::lock(conn, name, clientCookie1, duration, yield);
        ASSERT_EQ(return_code, 0);

        return_code =
            rgw::redislock::lock(conn, name, clientCookie2, duration, yield);
        ASSERT_EQ(return_code, -EBUSY);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisLockTest, Timeout) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int duration = 500;
        const std::string name = "lock:timeout";
        const std::string cookie = "mycookie";

        int return_code =
            rgw::redislock::lock(conn, name, cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        boost::asio::steady_timer timer(io, std::chrono::milliseconds(1000));
        timer.async_wait(yield);

        return_code = rgw::redislock::assert_locked(conn, name, cookie, yield);
        ASSERT_EQ(return_code, -ENOENT);

        const std::string new_cookie = "newcookie";
        return_code =
            rgw::redislock::lock(conn, name, new_cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        return_code =
            rgw::redislock::assert_locked(conn, name, new_cookie, yield);
        ASSERT_EQ(return_code, 0);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}
