#pragma once

#include "rdkafka.h"
#include "rdkafkacpp.h"

namespace RdKafka {
    class message final {
    public:
        message(rd_kafka_type_t rk_type, RdKafka::Topic *topic, rd_kafka_message_t *rkmessage, bool dofree) : topic_(topic),
                                                                                                              rkmessage_(rkmessage),
                                                                                                              free_rkmessage_(dofree),
                                                                                                              key_(nullptr),
                                                                                                              headers_(nullptr),
                                                                                                              rk_type_(rk_type) {}

    private:
        Topic *topic_;
        rd_kafka_message_t *rkmessage_;
        bool free_rkmessage_;
        mutable std::string *key_;
        Headers *headers_;
        const rd_kafka_type_t rk_type_;
    };
}
