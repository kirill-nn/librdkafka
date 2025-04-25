#pragma once

#include <memory>
#include <functional>

#include "common.hpp"
#include "rdkafka.h"

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

        ConfResult set(const std::string &name, const std::string &value, error_code& ec) const;

        [[nodiscard]] rd_kafka_conf_t *rk_conf() const;

        [[nodiscard]] rd_kafka_topic_conf_t *rkt_conf() const;

        [[nodiscard]] ConfType conf_type() const;

    private:
        explicit configuration(ConfType conf_type);

        ConfType conf_type_;
        rd_kafka_conf_t *rk_conf_;
        rd_kafka_topic_conf_t *rkt_conf_;
    };
}
