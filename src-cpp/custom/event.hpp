#pragma once
#include "rdkafkacpp.h"

namespace RdKafka {
    class event final {
    public:
        event(Type type, ErrorCode err, Severity severity, const char *fac, const char *str);

        explicit event(Type type);

        Type type() const;

        ErrorCode err() const;

        Severity severity() const;

        std::string fac() const;

        std::string str() const;

        std::string broker_name() const;

        int broker_id() const;

        int throttle_time() const;

        bool fatal() const;

    private:
        bool fatal_;
        Type type_;
        ErrorCode err_;
        Severity severity_;
        int id_;
        int throttle_time_;
        std::string fac_;
        std::string str_; /* reused for THROTTLE broker_name */
    };
}
