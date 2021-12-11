/**
 * @file validation.hpp
 * @author HUANG Qiyue
 * @brief
 * @version 0.1
 * @date 2021-12-06
 *
 * @copyright Copyright (c) 2021
 *
 */

#ifndef VALIDATION_H
#define VALIDATION_H

#include <algorithm>
#include <iostream>
#include <set>
#include <unordered_set>

/* test endianness of the platform */
void showEndianness() {
    uint32_t val = 0x01;
    char* buff = (char*)&val;
    if (buff[0] == 0) {
        std::cout << "Big endian\n";
    } else {
        std::cout << "Little endian\n";
    };
}

template <typename T>
bool is_sorted(const std::string input_name, bool ascending = true) {
    /* open file */
    std::ifstream input;
    input.open(input_name.c_str(), std::ios::in | std::ios::binary);

    if (!input.good()) {
        std::cerr << "Unable to read file " << input_name << " !" << std::endl;
        exit(-1);
    }

    T cur_data, last_data;
    input.read((char*)&last_data, sizeof(T));
    while (!input.eof()) {
        input.read((char*)&cur_data, sizeof(T));
        if (!input.eof()) {
            if (ascending) {
                if (cur_data < last_data) return false;
            } else {
                if (cur_data > last_data) return false;
            }
        }
        last_data = cur_data;
    }
    return true;
}

template <typename T>
bool is_consistent(const std::string input_name,
                   const std::string output_prefix, size_t start_run,
                   size_t end_run) {
    std::unordered_multiset<T> input_set;
    std::unordered_multiset<T> output_set;

    /* open input file */
    std::ifstream input;
    input.open(input_name.c_str(), std::ios::in | std::ios::binary);

    if (!input.good()) {
        std::cerr << "Unable to read file " << input_name << " !" << std::endl;
        exit(-1);
    }

    T cur_data;
    while (!input.eof()) {
        input.read((char*)&cur_data, sizeof(T));
        if(!input.eof()) input_set.insert(cur_data);
    }

    input.close();

    /* open output files */
    for (auto i = start_run; i <= end_run; ++i) {
        std::string filename = output_prefix + std::to_string(i);
        input.open(filename.c_str(), std::ios::in | std::ios::binary);

        if (!input.good()) {
            std::cerr << "Unable to read file " << filename << " !"
                      << std::endl;
            exit(-1);
        }

        T cur_data;
        while (!input.eof()) {
            input.read((char*)&cur_data, sizeof(T));
            if(!input.eof()) output_set.insert(cur_data);
        }

        input.close();
    }

    /*
    std::cout << "Size of input set: " << input_set.size()
              << ", size of output files: " << output_set.size() << std::endl;
    */

    return std::is_permutation(input_set.begin(), input_set.end(),
                               output_set.begin());
}

#endif