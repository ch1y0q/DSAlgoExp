/**
 * @file ext_quicksort.cpp
 * @author HUANG Qiyue
 * @brief
 * @version 0.1
 * @date 2021-11-13
 *
 * @copyright Copyright (c) 2021
 *
 */

/* Generate a randomly-filled binary file on Linux
    head -c 256KB </dev/urandom >data_chunk
*/

//#define DEBUG

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
unsigned long INPUT_BUFFER_MEM;
unsigned long INPUT_BUFFER_SIZE;
unsigned long SMALL_GROUP_MEM;
unsigned long SMALL_GROUP_SIZE;
unsigned long LARGE_GROUP_MEM;
unsigned long LARGE_GROUP_SIZE;
unsigned long MIDDLE_GROUP_MEM;
unsigned long MIDDLE_GROUP_SIZE;

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
void ext_qsort(TreeNode<std::string>* cur_node) {
    /* init parameters */
    std::string input_name = cur_node->data;
    INPUT_BUFFER_MEM = TOTAL_MEM * INPUT_BUFFER_PROPORTION;
    INPUT_BUFFER_SIZE = INPUT_BUFFER_MEM / sizeof(T);
    SMALL_GROUP_MEM = TOTAL_MEM * SMALL_GROUP_PROPORTION;
    SMALL_GROUP_SIZE = SMALL_GROUP_MEM / sizeof(T);
    LARGE_GROUP_MEM = LARGE_GROUP_MEM * LARGE_GROUP_PROPORTION;
    LARGE_GROUP_SIZE = LARGE_GROUP_MEM / sizeof(T);
    MIDDLE_GROUP_MEM =
        TOTAL_MEM - INPUT_BUFFER_MEM - SMALL_GROUP_MEM - LARGE_GROUP_MEM;
    MIDDLE_GROUP_SIZE = MIDDLE_GROUP_MEM / sizeof(T);

    /* open file */
    std::ifstream input;
    input.open(input_name.c_str(), std::ios::in | std::ios::binary);

    if (!input.good()) {
        std::cerr << "Unable to read file " << input_name << " !" << std::endl;
        exit(-1);
    }

    /* get size */
    unsigned int input_size;
    input.seekg(0, input.end);
    input_size = input.tellg();
    input.seekg(0, input.beg);
    PRINT_SEPARATOR_START;

    std::cout << "Reading file " << input_name << " !" << std::endl;
    std::cout << "Total size (in bytes): " << input_size << std::endl;
    std::cout << "Total items: " << input_size / sizeof(T) << std::endl;

    /* init reader */
    ext_qsort_t<T> manager(INPUT_BUFFER_SIZE, SMALL_GROUP_SIZE,
                           LARGE_GROUP_SIZE, MIDDLE_GROUP_SIZE);

    PRINT_SEPARATOR_END;

    PRINT_SEPARATOR_START;
    while (!input.eof()) {
        /* feed input_buffer */
        while (manager.input_buffer.size() < INPUT_BUFFER_SIZE &&
               !input.eof()) {
            T cur_data;
            input.read((char*)&cur_data, sizeof(T));
            disk_read_count++;
            if (!input.eof()) manager.input_buffer.push_back(cur_data);
        }

        /* digest fed data */
        while (manager.middle_group.size() < MIDDLE_GROUP_SIZE &&
               !manager.input_buffer.empty()) {
            T cur_data = manager.input_buffer.back();
            manager.input_buffer.pop_back();
            manager.middle_group.push(cur_data);
        }
        while (!manager.input_buffer.empty()) {
            T cur_data = manager.input_buffer.back();
            manager.input_buffer.pop_back();
            T largest = manager.middle_group.findMax();
            T smallest = manager.middle_group.findMin();
            if (cur_data > largest) {
                manager.large_group.push_back(cur_data);
            } else if (cur_data < smallest) {
                manager.small_group.push_back(cur_data);
            } else {  // between min and max
                /* balancing */
                if (manager.large_group.size() > manager.small_group.size()) {
                    auto ele = manager.middle_group.popMin();
                    manager.small_group.push_back(ele);
                    manager.middle_group.push(cur_data);
                } else {
                    auto ele = manager.middle_group.popMax();
                    manager.large_group.push_back(ele);
                    manager.middle_group.push(cur_data);
                }
            }

            if (manager.large_group.size() >= LARGE_GROUP_SIZE) {
                // save structure to binary tree
                auto save_name = cur_node->data + DUMPED_LARGE_SUFFIX;
                if (!cur_node->RightChild)
                    cur_node->RightChild = new TreeNode<std::string>(save_name);
                // write to disk!
                std::fstream fs(save_name, std::ios::out | std::ios::binary |
                                               std::ios::app);
                if (!fs) {
                    std::cerr << "Error opening file: " << save_name << "\n";
                }
                while (!manager.large_group.empty()) {
                    T data_to_save = manager.large_group.back();
                    manager.large_group.pop_back();
                    fs.write(reinterpret_cast<char*>(&data_to_save), sizeof(T));
                    disk_write_count++;
                }
                fs.close();
            }
            if (manager.small_group.size() >= SMALL_GROUP_SIZE) {
                // save structure to binary tree
                auto save_name = cur_node->data + DUMPED_SMALL_SUFFIX;
                if (!cur_node->LeftChild)
                    cur_node->LeftChild = new TreeNode<std::string>(save_name);
                // write to disk!
                std::fstream fs(save_name, std::ios::out | std::ios::binary |
                                               std::ios::app);
                if (!fs) {
                    std::cerr << "Error opening file: " << save_name << "\n";
                }
                while (!manager.small_group.empty()) {
                    T data_to_save = manager.small_group.back();
                    manager.small_group.pop_back();
                    fs.write(reinterpret_cast<char*>(&data_to_save), sizeof(T));
                    disk_write_count++;
                }
                fs.close();
            }
        }
    }

#ifdef DEBUG
    std::cout << "large group: " << manager.large_group.size() << std::endl;
    for (auto i : manager.large_group) {
        std::cout << i << " ";
    }
    std::cout << std::endl;

    std::cout << "small group: " << manager.small_group.size() << std::endl;
    for (auto i : manager.small_group) {
        std::cout << i << " ";
    }
    std::cout << std::endl;

    std::cout << "middle group: " << manager.middle_group.size() << std::endl;
    manager.middle_group.printRaw();
    std::cout << std::endl;
#endif

    /* write middle group to disk! */
    {
        std::fstream fs(cur_node->data,
                        std::ios::out | std::ios::binary);  // no std::ios::app
        if (!fs) {
            std::cerr << "Error opening file: " << cur_node->data << "\n";
        }

        while (!manager.middle_group.empty()) {
            T data_to_save = manager.middle_group.popMin();  // ascending order
            fs.write(reinterpret_cast<char*>(&data_to_save), sizeof(T));
            disk_write_count++;
        }
        fs.close();
    }

    /* write small and large groups to disk! */
    if (!manager.large_group.empty()) {
        // save structure to binary tree
        auto save_name = cur_node->data + DUMPED_LARGE_SUFFIX;
        if (!cur_node->RightChild)
            cur_node->RightChild = new TreeNode<std::string>(save_name);
        // write to disk!
        std::fstream fs(save_name,
                        std::ios::out | std::ios::binary | std::ios::app);
        if (!fs) {
            std::cerr << "Error opening file: " << save_name << "\n";
        }
        while (!manager.large_group.empty()) {
            T data_to_save = manager.large_group.back();
            manager.large_group.pop_back();
            fs.write(reinterpret_cast<char*>(&data_to_save), sizeof(T));
            disk_write_count++;
        }
        fs.close();
    }

    if (!manager.small_group.empty()) {
        // save structure to binary tree
        auto save_name = cur_node->data + DUMPED_SMALL_SUFFIX;
        if (!cur_node->LeftChild)
            cur_node->LeftChild = new TreeNode<std::string>(save_name);
        // write to disk!
        std::fstream fs(save_name,
                        std::ios::out | std::ios::binary | std::ios::app);
        if (!fs) {
            std::cerr << "Error opening file: " << save_name << "\n";
        }
        while (!manager.small_group.empty()) {
            T data_to_save = manager.small_group.back();
            manager.small_group.pop_back();
            fs.write(reinterpret_cast<char*>(&data_to_save), sizeof(T));
            disk_write_count++;
        }
        fs.close();
    }

#ifdef DEBUG
    std::cout << cur_node->data << " sorted? " << std::boolalpha
              << is_sorted<T>(cur_node->data) << std::endl;
#endif

    /* done */
    std::cout << "Read " << input_name << " done!" << std::endl;
    PRINT_TIME_SO_FAR;
    PRINT_SEPARATOR_END;

    /* recursively process large and small groups */
    if (cur_node->RightChild) {
        ext_qsort<T>(cur_node->RightChild);
    }
    if (cur_node->LeftChild) {
        ext_qsort<T>(cur_node->LeftChild);
    }
}

void merge(std::string& output_name, TreeNode<std::string>* cur_node) {
    if (cur_node) {
        merge(output_name, cur_node->LeftChild);

        std::ifstream src;
        std::ofstream dst;
        src.open(cur_node->data, std::ios::in | std::ios::binary);
        dst.open(output_name, std::ios::out | std::ios::binary | std::ios::app);
        dst << src.rdbuf();
        src.close();
        dst.close();

        merge(output_name, cur_node->RightChild);
    }
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
    TOTAL_MEM = 1024*16;

    disk_read_count = disk_write_count = 0;

    auto* root_node = new TreeNode<std::string>(input_name);

    ext_qsort<uint32_t>(root_node);
    merge(output_name, root_node);

    std::cout << "Finished.\n";
    std::cout << "Disk read count: " << disk_read_count << std::endl;
    std::cout << "Disk write count: " << disk_write_count << std::endl;
    PRINT_TIME_SO_FAR;
    return 0;
}