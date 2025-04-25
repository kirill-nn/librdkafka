#include <configuration.hpp>
#include <iostream>
#include <ostream>

#include "producer.hpp"

using namespace RdKafka;

int main(int argc, char **argv) {
    auto configuration = configuration::create(configuration::ConfType::CONF_GLOBAL);

    error_code ec {};
    std::string topic {"test_topic"};
    std::string brokers {"localhost:29092"};
    std::string line {"message to send"};

    if (configuration->set("bootstrap.servers", brokers, ec) != configuration::ConfResult::CONF_OK) {
        std::cerr << ec.what() << std::endl;
        return EXIT_FAILURE;
    }

    const auto producer = producer::create(std::move(configuration), ec)
        .with_log_callback([] (int level, const char *fac, const char *buf) { printf("%s\n", buf); })
        .build();

    if (ec) {
        std::cerr << ec.what() << std::endl;
        return EXIT_FAILURE;
    }

    if (ErrorCode err = producer->produce(topic, Topic::PARTITION_UA, producer::RK_MSG_COPY, const_cast<char *>(line.c_str()), line.size(), nullptr, 0, 0, nullptr, nullptr); err != ERR_NO_ERROR) {
        std::cerr << rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(err)) << std::endl;
    }

    getchar();
}
