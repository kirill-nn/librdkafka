#pragma once

namespace RdKafka {

    struct error_code final {

        error_code() : buffer{} {}

        [[nodiscard]] const char* what() const noexcept { return buffer.data(); }

        char* data() { return buffer.data(); }

        operator bool() const { return buffer[0] != 0; }

        void operator=(const char* source) { std::copy_n(source, strlen(source), buffer.data()); };

    private:
        std::array<char, 512> buffer;
    };
}
