//
// Created by Kirill on 24/04/2025.
//

#include "configuration.hpp"

std::unique_ptr<RdKafka::configuration> RdKafka::configuration::create(ConfType conf_type) {
    std::unique_ptr<configuration> conf {new configuration(conf_type)};

    if (conf_type == ConfType::CONF_GLOBAL)
        conf->rk_conf_ = rd_kafka_conf_new();
    else
        conf->rkt_conf_ = rd_kafka_topic_conf_new();

    return conf;
}

rd_kafka_conf_t * RdKafka::configuration::rk_conf() const { return rk_conf_; }

rd_kafka_topic_conf_t * RdKafka::configuration::rkt_conf() const { return rkt_conf_; }

std::function<void(std::unique_ptr<RdKafka::message>)> RdKafka::configuration::dr_cb() const { return dr_cb_;}

RdKafka::configuration::configuration(ConfType conf_type) : rk_conf_(nullptr), rkt_conf_(nullptr) {}
