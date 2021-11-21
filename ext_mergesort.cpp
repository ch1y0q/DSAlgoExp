/**
 * @file ext_mergesort.cpp
 * @author HUANG Qiyue
 * @brief
 * @version 0.1
 * @date 2021-11-17
 *
 * @copyright Copyright (c) 2021
 *
 */

//#define DEBUG
#define FINAL_CHECK

#include <algorithm>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>

#include "defs.h"
#include "structures.h"

#define PRINT_TIME_SO_FAR                                            \
    std::cout << "Elasped time: "                                    \
              << float(clock() - begin_time) / CLOCKS_PER_SEC * 1000 \
              << " msec." << std::endl;
#define PRINT_SEPARATOR_START \
    std::cout << "-------------------------------------------------------\n";
#define PRINT_SEPARATOR_END \
    std::cout               \
        << "-------------------------------------------------------\n\n\n";

const clock_t begin_time = clock();  // time at initialization

unsigned long disk_read_count = 0;
unsigned long disk_write_count = 0;

unsigned long TOTAL_MEM;

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
void dump_to_file(std::vector<T>& data, ssize_t run_count) {
    /* sort NOT done here */
    std::string ss(DUMPED_RUN_PREFIX);
    ss += std::to_string(run_count);
    std::cout << "Writing " << ss << std::endl;

    std::fstream output(ss, std::ios::out | std::ios::binary);

    int data_size = data.size();
    for (int i = 0; i < data_size; i++) {
        auto data_to_save = data[i];
        output.write(reinterpret_cast<char*>(&data_to_save), sizeof(T));
        disk_write_count++;
    }
    output.close();
}

template <typename T>
/* generate the initial runs */
ssize_t input(const char* filename, int total_mem) {
    std::ifstream fs;
    fs.open(filename, std::ios::in | std::ios::binary);

    if (!fs.good()) {
        std::cerr << "File " << filename << " not found." << std::endl;
        exit(-1);
    }
    fs.seekg(0, fs.end);
    ssize_t input_size = fs.tellg();
    fs.seekg(0, fs.beg);

    PRINT_SEPARATOR_START;
    std::cout << "Reading file " << filename << " !" << std::endl;
    std::cout << "Total size (in bytes): " << input_size << std::endl;
    std::cout << "Total items: " << input_size / sizeof(T) << std::endl;
    PRINT_SEPARATOR_END;

    ssize_t run_count = 0;
    int total_used_mem = 0;

    /* init output buffer */
    std::vector<T> data;
    data.clear();

    while (!fs.eof()) {
        T cur_data;
        fs.read((char*)&cur_data, sizeof(T));
        disk_read_count++;
        if (!fs.eof()) {
            if (total_used_mem + sizeof(T) < total_mem) {
                /* insert and keep ordered */
                auto const insertion_point =
                    std::upper_bound(data.begin(), data.end(), cur_data);
                data.insert(insertion_point, cur_data);

                total_used_mem += sizeof(T);
            } else {
                dump_to_file(data, ++run_count);
                data.clear();
                total_used_mem = sizeof(T);
                data.push_back(cur_data);  // first element, no need to sort
            }
        }
    }

    if (!data.empty()) {
        dump_to_file(data, ++run_count);
    }
    fs.close();

#ifdef DEBUG
    for (int i = 1; i <= run_count; ++i) {
        std::cout << "run_" << i << " sorted? " << std::boolalpha
                  << is_sorted<T>(DUMPED_RUN_PREFIX + std::to_string(i))
                  << std::endl;
    }
#endif

    /* done */
    std::cout << "Generating initial runs for " << filename << " done!"
              << std::endl;
    PRINT_TIME_SO_FAR;
    PRINT_SEPARATOR_END;

    return run_count;
}

template <typename T>
ssize_t input(std::string filename, int total_mem) {
    return input<T>(filename.c_str(), total_mem);
}

template <typename T>
void merge(int start, int end, int location, int total_mem) {
    int runs_count = end - start + 1;

    std::ifstream input[runs_count];
    for (int i = 0; i < runs_count; i++) {
        std::string ss(DUMPED_RUN_PREFIX);
        ss += std::to_string(start + i);
        input[i].open(ss, std::ios::in | std::ios::binary);
    }

    /* std::greater for min heap */
    std::priority_queue<HeapNode<T>, std::vector<HeapNode<T>>,
                        std::greater<HeapNode<T>>>
        heap;

    std::ofstream output;
    std::string ss(DUMPED_RUN_PREFIX);
    ss += std::to_string(location);
    output.open(ss, std::ios::out | std::ios::binary | std::ios::ate);

    for (int i = 0; i < runs_count; i++) {
        T cur_data;
        if (!input[i].eof()) {
            input[i].read((char*)&cur_data, sizeof(T));
            disk_read_count++;
            if (!input[i].eof()) heap.push(HeapNode(cur_data, i));
            if (heap.size() >= total_mem / sizeof(T)) {
                while (!heap.empty()) {
                    T to_pop = heap.top().data;
                    heap.pop();

                    output.write(reinterpret_cast<char*>(&to_pop), sizeof(T));
                    disk_write_count++;
                }
            }
        }
    }

    PRINT_SEPARATOR_START;
    std::cout << "\nMerging from " << DUMPED_RUN_PREFIX << start << " to "
              << end << " into " << DUMPED_RUN_PREFIX << location << " file"
              << std::endl;

    while (!heap.empty()) {
        T cur_data = heap.top().data;
        int index = heap.top().index;
        heap.pop();

        output.write(reinterpret_cast<char*>(&cur_data), sizeof(T));
        disk_write_count++;
        if (!input[index].eof()) {
            input[index].read((char*)&cur_data, sizeof(T));
            disk_read_count++;
            if (!input[index].eof()) heap.push(HeapNode(cur_data, index));
        }
    }

    std::cout << "Merge done!\n\n";
    PRINT_SEPARATOR_END;

    for (int i = 0; i < runs_count; i++) {
        input[i].close();
    }

    output.close();
}

template <typename T>
void merge(int runs_count, std::string output_name, int total_mem) {
    PRINT_SEPARATOR_START;
    std::cout << "Merging " << runs_count << " files into \"" << output_name
              << "\"" << std::endl;
    PRINT_SEPARATOR_END;

    int start = 1;
    int end = runs_count;
    while (start < end) {
        int location = end;
        int distance = 1;  // 1 for pairwise merging
        int time = (end - start + 1) / distance + 1;
        if ((end - start + 1) / time < distance) {
            distance = (end - start + 1) / time + 1;
        }
        while (start <= end) {
            int mid = std::min(start + distance, end);
            location++;
            merge<T>(start, mid, location, total_mem);
            start = mid + 1;
        }
        end = location;
    }

    std::string ss(DUMPED_RUN_PREFIX);
    ss += std::to_string(start);
    rename(ss.c_str(), output_name.c_str());

    PRINT_SEPARATOR_START;
    std::cout << "Removing chucks files!" << std::endl;
    for (int i = 1; i < end; i++) {
        std::string ss(DUMPED_RUN_PREFIX);
        ss += std::to_string(i);
        std::cout << "Removing " << ss << std::endl;
        remove(ss.c_str());
    }
    PRINT_SEPARATOR_END;
}

int main(int argc, char* argv[]) {
    /*
    if (argc < 4) {
        std::cout << "Usage: input_file output_file mem_size" << std::endl;
        return 0;
    }

    std::string input_name = argv[1];
    std::string output_name = argv[2];
    TOTAL_MEM = strtol(argv[3], nullptr, 0); // available memory in bytes
    */

#ifdef DEBUG
    /* test endianness of the platform */
    {
        uint32_t val = 0x01;
        char* buff = (char*)&val;

        if (buff[0] == 0) {
            std::cout << "Big endian\n";
        } else {
            std::cout << "Little endian\n";
        };
    }
#endif

    std::string input_name = "data_chunk_256KB";
    std::string output_name = "data_chunk_256KB_sorted";
    TOTAL_MEM = 250;

    disk_read_count = disk_write_count = 0;

    auto runs_count = input<uint32_t>(input_name, TOTAL_MEM);
    merge<uint32_t>(runs_count, output_name, TOTAL_MEM);

    std::cout << "Finished.\n";
    std::cout << "Disk read count: " << disk_read_count << std::endl;
    std::cout << "Disk write count: " << disk_write_count << std::endl;
    PRINT_TIME_SO_FAR;

#ifdef FINAL_CHECK
    std::cout << output_name << " sorted? " << std::boolalpha
              << is_sorted<uint32_t>(output_name) << std::endl;
#endif

    return 0;
}