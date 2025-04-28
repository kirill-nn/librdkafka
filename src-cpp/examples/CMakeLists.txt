if(WIN32)
    set(win32_sources ../../win32/wingetopt.c ../../win32/wingetopt.h)
endif(WIN32)

add_executable(custom_producer custom_producer.cpp ${win32_sources})
target_link_libraries(custom_producer PUBLIC rdkafka++)

target_include_directories(custom_producer PUBLIC ../custom)