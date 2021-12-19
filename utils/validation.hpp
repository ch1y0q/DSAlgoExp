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

#include "../structures.hpp"

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
                if (cur_data < last_data) {
                    input.close();
                    return false;
                }
            } else {
                if (cur_data > last_data) {
                    input.close();
                    return false;
                }
            }
        }
        last_data = cur_data;
    }
    input.close();
    return true;
}

template <typename T>
void is_sorted(const std::string prefix, const run_len_pairs run_len,
               bool ascending = true) {
    for (auto i : run_len) {
        std::string filename = prefix + std::to_string(i.first);
        std::cout << filename << " sorted? " << std::boolalpha
                  << is_sorted<T>(filename, ascending) << std::endl;
    }
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
        if (!input.eof()) input_set.insert(cur_data);
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
            if (!input.eof()) output_set.insert(cur_data);
        }

        input.close();
    }

    /*
    std::cout << "Size of input set: " << input_set.size()
              << ", size of output files: " << output_set.size() << std::endl;
    */

    return input_set == output_set;
}

template <typename T>
bool is_consistent(const std::string input_name,
                   const std::string output_prefix,
                   const run_len_pairs run_len) {
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
        if (!input.eof()) input_set.insert(cur_data);
    }

    input.close();

    /* open output files */
    for (auto i : run_len) {
        std::string filename = output_prefix + std::to_string(i.first);
        input.open(filename.c_str(), std::ios::in | std::ios::binary);

        if (!input.good()) {
            std::cerr << "Unable to read file " << filename << " !"
                      << std::endl;
            exit(-1);
        }

        T cur_data;
        while (!input.eof()) {
            input.read((char*)&cur_data, sizeof(T));
            if (!input.eof()) output_set.insert(cur_data);
        }

        input.close();
    }

    /*
    std::cout << "Size of input set: " << input_set.size()
              << ", size of output files: " << output_set.size() << std::endl;
    */

    return input_set == output_set;
}

template <typename T>
bool is_consistent(const std::string prefix, const run_len_pairs& run_len,
                   const size_t output_run) {
    std::unordered_multiset<T> input_set;
    std::unordered_multiset<T> output_set;

    /* open input file */
    std::ifstream input;

    for (auto i : run_len) {
        std::string input_name = prefix + std::to_string(i.first);
        input.open(input_name.c_str(), std::ios::in | std::ios::binary);

        if (!input.good()) {
            std::cerr << "Unable to read file " << input_name << " !"
                      << std::endl;
            exit(-1);
        }

        T cur_data;
        while (!input.eof()) {
            input.read((char*)&cur_data, sizeof(T));
            if (!input.eof()) input_set.insert(cur_data);
        }
        input.close();
    }

    /* open output file */

    std::string filename = prefix + std::to_string(output_run);
    input.open(filename.c_str(), std::ios::in | std::ios::binary);

    if (!input.good()) {
        std::cerr << "Unable to read file " << filename << " !" << std::endl;
        exit(-1);
    }

    T cur_data;
    while (!input.eof()) {
        input.read((char*)&cur_data, sizeof(T));
        if (!input.eof()) output_set.insert(cur_data);
    }

    input.close();

    /*
    std::cout << "Size of input set: " << input_set.size()
              << ", size of output files: " << output_set.size() << std::endl;
    */

    return input_set == output_set;
}

#endif