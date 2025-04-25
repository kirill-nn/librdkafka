#include "configuration.hpp"

std::unique_ptr<RdKafka::configuration> RdKafka::configuration::create(ConfType conf_type) {
    std::unique_ptr<configuration> conf {new configuration(conf_type)};

    if (conf_type == ConfType::CONF_GLOBAL)
        conf->rk_conf_ = rd_kafka_conf_new();
    else
        conf->rkt_conf_ = rd_kafka_topic_conf_new();

    return conf;
}

RdKafka::configuration::ConfResult RdKafka::configuration::set(const std::string &name, const std::string &value, error_code& ec) const {
    rd_kafka_conf_res_t res;

    if (this->conf_type_ == ConfType::CONF_GLOBAL)
        res = rd_kafka_conf_set(this->rk_conf_, name.c_str(), value.c_str(), ec.data(),
                                sizeof(ec));
    else
        res = rd_kafka_topic_conf_set(this->rkt_conf_, name.c_str(), value.c_str(),
                                      ec.data(), sizeof(ec));

    return static_cast<ConfResult>(res);
}

rd_kafka_conf_t * RdKafka::configuration::rk_conf() const { return rk_conf_; }

rd_kafka_topic_conf_t * RdKafka::configuration::rkt_conf() const { return rkt_conf_; }

RdKafka::configuration::ConfType RdKafka::configuration::conf_type() const { return conf_type_;}

RdKafka::configuration::configuration(ConfType conf_type) : conf_type_(conf_type), rk_conf_(nullptr), rkt_conf_(nullptr) {}
