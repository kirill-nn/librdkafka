#pragma once

#include <memory>
#include <functional>

#include "rdkafka.h"
#include "message.hpp"

namespace RdKafka {
    class configuration final {
    public:
        enum class ConfType {
            CONF_GLOBAL, /**< Global configuration */
            CONF_TOPIC /**< Topic specific configuration */
        };

        enum class ConfResult {
            CONF_UNKNOWN = -2, /**< Unknown configuration property */
            CONF_INVALID = -1, /**< Invalid configuration value */
            CONF_OK = 0 /**< Configuration property was succesfully set */
        };

        static std::unique_ptr<configuration> create(ConfType conf_type);

        [[nodiscard]] rd_kafka_conf_t* rk_conf() const;

        [[nodiscard]] rd_kafka_topic_conf_t* rkt_conf() const;

        std::function<void(std::unique_ptr<message>)> dr_cb() const;
    private:
        explicit configuration(ConfType conf_type);

        rd_kafka_conf_t *rk_conf_;
        rd_kafka_topic_conf_t *rkt_conf_;
        std::function<void(std::unique_ptr<message>)> dr_cb_;
    };
}
