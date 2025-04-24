#include "producer.hpp"
#include "rdkafkacpp_int.h"

#include <memory>

namespace {
    void dr_msg_cb_trampoline(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
        const auto* handle = static_cast<RdKafka::producer *>(opaque);
        auto message = std::make_unique<RdKafka::message>(RD_KAFKA_PRODUCER, nullptr, const_cast<rd_kafka_message_t *>(rkmessage), false);
        handle->callback(std::move(message));
    }
}

std::unique_ptr<RdKafka::producer> RdKafka::producer::create(std::unique_ptr<configuration> conf, std::string &errstr) {
    std::unique_ptr<producer> rkp {new producer()};
    rd_kafka_conf_t *rk_conf = nullptr;

    if (conf) {
        if (!conf->rk_conf()) {
            errstr = "Requires RdKafka::Conf::CONF_GLOBAL object";
            return rkp;
        }

        rkp->set_config(conf.get());

        rk_conf = rd_kafka_conf_dup(conf->rk_conf());

        rd_kafka_conf_set_dr_msg_cb(rk_conf, dr_msg_cb_trampoline);
        rkp->dr_cb_ = conf->dr_cb();
    }

    char errbuf[512] {};
    if (rd_kafka_t *rk; !(rk = rd_kafka_new(RD_KAFKA_PRODUCER, rk_conf, errbuf, sizeof(errbuf)))) {
        errstr = errbuf;
        // rd_kafka_new() takes ownership only if succeeds
        if (rk_conf)
            rd_kafka_conf_destroy(rk_conf);
    } else {
        rkp->rk_ = rk;
    }
    return rkp;
}

RdKafka::ErrorCode RdKafka::producer::produce(Topic *topic, int32_t partition, const std::vector<char> *payload, const std::vector<char> *key, void *msg_opaque) {
    //TODO tbd
    return ERR_NO_ERROR;
}

void RdKafka::producer::callback(std::unique_ptr<message> message) const {
    dr_cb_(std::move(message));
}

RdKafka::producer::producer() : rk_(nullptr) {}

void RdKafka::producer::set_config(const configuration *conf) {
    //TODO tbd
}
