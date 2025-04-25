#include "producer.hpp"
#include "rdkafkacpp_int.h"

#include <memory>
#include <utility>

namespace {
    void dr_msg_cb_trampoline(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
        auto *handle = static_cast<RdKafka::producer *>(opaque);
        auto message = std::make_unique<RdKafka::message>(RD_KAFKA_PRODUCER, nullptr, const_cast<rd_kafka_message_t *>(rkmessage), false);
        handle->delivery_callback()(std::move(message));
    }

    void log_callback_trampoline(const rd_kafka_t *rk, int level, const char *fac, const char *buf) {
        if (!rk) {
            rd_kafka_log_print(rk, level, fac, buf);
            return;
        }

        void *opaque = rd_kafka_opaque(rk);
        auto *handle = static_cast<RdKafka::producer *>(opaque);
        handle->log_callback()(level, fac, buf);
    }

    void error_callback_trampoline(rd_kafka_t *rk, int err, const char *reason, void *opaque) {
        const auto *handle = static_cast<RdKafka::producer *>(opaque);
        //handle->callback(err, reason, reason);
    }

    void throttle_callback_trampoline(rd_kafka_t *rk, const char *broker_name, int32_t broker_id, int throttle_time_ms, void *opaque) {}

    int stats_callback_trampoline(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) { return 0; }
}

RdKafka::ErrorCode RdKafka::producer::produce(std::string topic_name, int32_t partition, int msgflags, void *payload, size_t len, const void *key, size_t key_len, int64_t timestamp, Headers *headers, void *msg_opaque) const {
    rd_kafka_headers_t *hdrs = nullptr;
    HeadersImpl *headersimpl = nullptr;

    if (headers) {
        headersimpl = static_cast<HeadersImpl *>(headers);
        hdrs = headersimpl->c_ptr();
    }

    rd_kafka_resp_err_t err = rd_kafka_producev(rk_, RD_KAFKA_V_TOPIC(topic_name.c_str()), RD_KAFKA_V_PARTITION(partition), RD_KAFKA_V_MSGFLAGS(msgflags), RD_KAFKA_V_VALUE(payload, len), RD_KAFKA_V_KEY(key, key_len), RD_KAFKA_V_TIMESTAMP(timestamp), RD_KAFKA_V_OPAQUE(msg_opaque),
                                                RD_KAFKA_V_HEADERS(hdrs), RD_KAFKA_V_END);

    if (!err && headersimpl) {
        headersimpl->c_headers_destroyed();
        delete headers;
    }

    return static_cast<ErrorCode>(err);
}

RdKafka::producer::delivery_callback_t RdKafka::producer::delivery_callback() { return dr_cb_; }

RdKafka::producer::log_callback_t RdKafka::producer::log_callback() { return log_cb_; }

RdKafka::producer::producer(std::unique_ptr<configuration> conf, error_code &ec, delivery_callback_t delivery_callback, log_callback_t log_callback) : rk_(nullptr),
                                                                                                                                                       dr_cb_(std::move(delivery_callback)),
                                                                                                                                                       log_cb_(std::move(log_callback)) {
    rd_kafka_conf_t *rk_conf = nullptr;

    if (conf) {
        if (!conf->rk_conf()) {
            ec = "Requires RdKafka::Conf::CONF_GLOBAL object";
            return;
        }

        // callbacks
        rd_kafka_conf_set_opaque(conf->rk_conf(), this);
        rd_kafka_conf_set_log_cb(conf->rk_conf(), log_callback_trampoline);
        rd_kafka_conf_set_error_cb(conf->rk_conf(), error_callback_trampoline);
        rd_kafka_conf_set_throttle_cb(conf->rk_conf(), throttle_callback_trampoline);
        rd_kafka_conf_set_stats_cb(conf->rk_conf(), stats_callback_trampoline);

        rk_conf = rd_kafka_conf_dup(conf->rk_conf());
        rd_kafka_conf_set_dr_msg_cb(rk_conf, dr_msg_cb_trampoline);
    }

    if (rd_kafka_t *rk; !(rk = rd_kafka_new(RD_KAFKA_PRODUCER, rk_conf, ec.data(), sizeof(ec)))) {
        // rd_kafka_new() takes ownership only if succeeds
        if (rk_conf)
            rd_kafka_conf_destroy(rk_conf);
    } else {
        rk_ = rk;
    }
}
