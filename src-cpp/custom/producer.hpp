#pragma once

#include "configuration.hpp"
#include "message.hpp"
#include "rdkafka.h"
#include "rdkafkacpp.h"

namespace RdKafka {
    class producer final {
    public:
        enum {
            RK_MSG_FREE = 0x1, /**< rdkafka will free(3) \p payload
                                * when it is done with it.
                                * Mutually exclusive with RK_MSG_COPY. */
            RK_MSG_COPY = 0x2, /**< the \p payload data will be copied
                                * and the \p payload pointer will not
                                * be used by rdkafka after the
                                * call returns.
                                * Mutually exclusive with RK_MSG_FREE. */
            RK_MSG_BLOCK = 0x4 /**< Block produce*() on message queue
                                *   full.
                                *   WARNING:
                                *   If a delivery report callback
                                *   is used the application MUST
                                *   call rd_kafka_poll() (or equiv.)
                                *   to make sure delivered messages
                                *   are drained from the internal
                                *   delivery report queue.
                                *   Failure to do so will result
                                *   in indefinately blocking on
                                *   the produce() call when the
                                *   message queue is full.
                                */
            /* For backwards compatibility: */
#ifndef MSG_COPY /* defined in sys/msg.h */
            , /** this comma must exist between
                          *  RK_MSG_BLOCK and MSG_FREE
                          */
            MSG_FREE = RK_MSG_FREE,
            MSG_COPY = RK_MSG_COPY
#endif
        };

        using delivery_callback_t = std::function<void(std::unique_ptr<message>)>;
        using log_callback_t = std::function<void(int level, const char *fac, const char *buf)>;
        using error_callback_t = std::function<void(int err, const char *reason)>;
        using throttle_callback_t = std::function<void(const char *broker_name, int32_t broker_id, int throttle_time_ms)>;
        using stats_callback_t = std::function<int(char *json, size_t json_len)>;

        class builder final {
        public:
            builder &with_delivery_callback(const delivery_callback_t &callback) {
                delivery_callback_ = callback;
                return *this;
            }

            builder &with_log_callback(const log_callback_t &callback) {
                log_callback_ = callback;
                return *this;
            }

            std::unique_ptr<producer> build() {
                return std::unique_ptr<producer>(new producer(std::move(conf_), ec_, delivery_callback_, log_callback_));
            }

        private:
            friend class producer;

            builder(std::unique_ptr<configuration> conf, error_code &ec) : delivery_callback_(delivery_callback_default),
                                                                           log_callback_(log_callback_default),
                                                                           conf_(std::move(conf)),
                                                                           ec_(ec) {}

            delivery_callback_t delivery_callback_default {[] (std::unique_ptr<message>) {}};
            log_callback_t log_callback_default {[] (int level, const char *fac, const char *buf) {}};

            delivery_callback_t delivery_callback_;
            log_callback_t log_callback_;

            std::unique_ptr<configuration> conf_;
            error_code &ec_;
        };

        static builder create(std::unique_ptr<configuration> conf, error_code &ec) {
            return {std::move(conf), ec};
        }

        ErrorCode produce(std::string topic_name, int32_t partition, int msgflags, void *payload, size_t len, const void *key, size_t key_len, int64_t timestamp, Headers *headers, void *msg_opaque) const;

        delivery_callback_t delivery_callback();

        log_callback_t log_callback();

    private:
        producer(std::unique_ptr<configuration> conf, error_code &ec, delivery_callback_t delivery_callback, log_callback_t log_callback);

        rd_kafka_t *rk_;
        delivery_callback_t dr_cb_;
        log_callback_t log_cb_;
    };
}
