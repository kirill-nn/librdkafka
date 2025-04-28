#pragma once

#include "rdkafkacpp.h"

namespace RdKafka {
    class event final {
    public:
        event(Event::Type type, ErrorCode err, Event::Severity severity, const char *fac, const char *str);

        explicit event(Event::Type type);

        Event::Type type() const;

        ErrorCode err() const;

        Event::Severity severity() const;

        std::string fac() const;

        std::string str() const;

        std::string broker_name() const;

        int broker_id() const;

        int throttle_time() const;

        bool fatal() const;

    private:
        bool fatal_;
        Event::Type type_;
        ErrorCode err_;
        Event::Severity severity_;
        int id_;
        int throttle_time_;
        std::string fac_;
        std::string str_; /* reused for THROTTLE broker_name */
    };
}
