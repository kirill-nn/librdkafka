#pragma once

#include "configuration.hpp"
#include "rdkafka.h"
#include "rdkafkacpp.h"

namespace RdKafka {

    class producer final {
    public:
        static std::unique_ptr<producer> create(std::unique_ptr<configuration> conf, std::string &errstr);

        ErrorCode produce(Topic *topic, int32_t partition, const std::vector<char> *payload, const std::vector<char> *key, void *msg_opaque);

        void callback(std::unique_ptr<message> message) const;
    private:
        producer();

        void set_config(const configuration* conf);

        rd_kafka_t *rk_;
        std::function<void(std::unique_ptr<message>)> dr_cb_;
    };
}
