/**
 * @file parallel_extsort.cpp
 * @author HUANG Qiyue
 * @brief
 * @version 0.1
 * @date 2021-12-01
 *
 * Phase 4: Merge Sort: Improve Run Generation
 * Three threads: Fetch from file, sort and output, run in parallel.
 * Sort is done with loser tree.
 *
 * @copyright Copyright (c) 2021
 *
 */

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>

#include "LoserTree.hpp"
#include "defs.h"
#include "structures.hpp"
#include "utils/stats.hpp"
#include "utils/validation.hpp"

//#define FINAL_CHECK
//#define DEBUG_COUT_ENABLED

#ifdef DEBUG_COUT_ENABLED
#define DEBUG_COUT(msg...) \
    printf(msg);           \
    fflush(stdout);
#else
#define DEBUG_COUT(msg...)
#endif

unsigned long disk_read_count = 0;
unsigned long disk_write_count = 0;

const size_t BLOCK_SIZE = 10000;

std::fstream input_fs;
std::fstream output_fs;

std::mutex mut_read_sort;
std::mutex mut_sort_write;
std::mutex mut_runs_count;
std::condition_variable reader_cond;
std::condition_variable writer_cond;
std::condition_variable sort_cond;

QueueBuffer<uint32_t> buffer1(BLOCK_SIZE);
QueueBuffer<uint32_t> buffer2(BLOCK_SIZE);
QueueBuffer<uint32_t> buffer3(BLOCK_SIZE);

/* shared by threads */
QueueBuffer<uint32_t>* read_buffer = &buffer1;
QueueBuffer<uint32_t>* sort_buffer = &buffer2;
QueueBuffer<uint32_t>* write_buffer = &buffer3;

/* states for synchronization */
bool is_writing = false;
size_t runs_count = SIZE_MAX;
size_t read_runs = 0;
size_t sorted_runs = 0;
size_t written_runs = 0;

run_len_pq length_per_run([](const run_len_pair& a, const run_len_pair& b) {
    return a.second > b.second;
});

template <typename T>
/* generate the initial runs */
void reader_function(const char* filename) {
    DEBUG_COUT("[READER] Reader thread is at your service.\n");

    input_fs.open(filename, std::ios::in | std::ios::binary);

    if (!input_fs.good()) {
        std::cerr << "File " << filename << " not found." << std::endl;
        exit(-1);
    }
    input_fs.seekg(0, input_fs.end);
    ssize_t input_size = input_fs.tellg();
    input_fs.seekg(0, input_fs.beg);

    PRINT_SEPARATOR_START;
    std::cout << "Reading file " << filename << " !" << std::endl;
    std::cout << "Total size (in bytes): " << input_size << std::endl;
    std::cout << "Total items: " << input_size / sizeof(T) << std::endl;
    PRINT_SEPARATOR_END;

    /* init read buffer */
    read_buffer->clear();

    while (!input_fs.eof()) {
        T cur_data;
        input_fs.read((char*)&cur_data, sizeof(T));
        disk_read_count++;
        if (!input_fs.eof()) {  // cur_data is valid
            std::unique_lock<std::mutex> lk(mut_read_sort);
            read_buffer->push(cur_data);
            // DEBUG_COUT("[READER] Read: %u\n", cur_data); // output each item
            // read is so time-consuming
        }
        if (read_buffer->full()) {
            std::unique_lock<std::mutex> lk(mut_read_sort);
            read_runs++;
            DEBUG_COUT(
                "[READER] run #%d read, read_buffer full, notify "
                "reader_cond.\n",
                read_runs);
            lk.unlock();
            reader_cond.notify_one();  // notify sorting thread to start working
            DEBUG_COUT("[READER] Waiting for reader_cond.\n", read_runs);

            lk.lock();
            reader_cond.wait(lk, []() { return (read_runs == sorted_runs); });
        }
    }

    input_fs.close();

    if (!read_buffer->empty()) {  // remaining elements in the buffer
        std::unique_lock<std::mutex> lk(mut_read_sort);
        read_runs++;
        DEBUG_COUT(
            "[READER] %d remainders as run #%d, notifying reader_cond.\n",
            read_buffer->getSize(), read_runs);
        lk.unlock();
        reader_cond.notify_one();
    }

    runs_count = read_runs;

    reader_cond.notify_one();  // alert end of runs
    writer_cond.notify_one();  // alert end of runs

    /* done */
    printf("Reader thread: %s finished reading!\n", filename);
    // PRINT_TIME_SO_FAR;
    // PRINT_SEPARATOR_END;
}

template <typename T>
void sort_function() {
    DEBUG_COUT("[SORT] Sort thread is at your service.\n");
    // reader_cond.notify_one();   // reader can start working!
    while (true) {
        DEBUG_COUT("[SORT] Fisrt line of loop!\n");
        std::unique_lock<std::mutex> lk_in(mut_read_sort);

        DEBUG_COUT("[SORT] Waiting for reader_cond.\n");
        reader_cond.wait(lk_in, []() {
            return read_runs > sorted_runs || sorted_runs == runs_count;
        });
        if (sorted_runs == runs_count) {
            break;
        }
        std::swap(read_buffer,
                  sort_buffer);  // exchange pointers to the two buffers

        DEBUG_COUT("[SORT] Swapped read_buffer and sort_buffer.\n");
        lk_in.unlock();

        std::vector<uint32_t> queue_vec;
        while (!sort_buffer->empty()) {
            queue_vec.push_back(sort_buffer->getNext());
        }
        std::sort(queue_vec.begin(), queue_vec.end());
        for (auto item : queue_vec) {
            sort_buffer->push(item);
        }

        lk_in.lock();
        sorted_runs++;
        lk_in.unlock();
        DEBUG_COUT("[SORT] Sorting sort_buffer done for run #%d on %d items.\n",
                   sorted_runs, sort_buffer->getSize());
        reader_cond.notify_one();

        std::unique_lock<std::mutex> lk_out(mut_sort_write);
        DEBUG_COUT("[SORT] Waiting for sort_cond.\n");
        sort_cond.wait(lk_out, []() {
            return !is_writing && sorted_runs == written_runs + 1;
        });
        std::swap(write_buffer,
                  sort_buffer);  // exchange pointers to two buffers
        DEBUG_COUT("[SORT] Swapped write_buffer and sort_buffer.\n");
        lk_out.unlock();

        writer_cond.notify_one();
        DEBUG_COUT("[SORT] Notifying writer_cond.\n");

        if (sorted_runs == runs_count) {
            DEBUG_COUT("[SORT] last_sort_done = true, break.\n");
            break;
        }
    }
    /* done */
    printf("Sorting thread: finished sorting!\n");
    // PRINT_TIME_SO_FAR;
    // PRINT_SEPARATOR_END;
}

template <typename T>
void writer_function(std::string filename_prefix) {
    DEBUG_COUT("[WRITER] Writer thread is at your service.\n");
    while (true) {
        std::unique_lock<std::mutex> lk_out(mut_sort_write);
        DEBUG_COUT("[WRITER] Waiting for writer_cond.\n");
        writer_cond.wait(lk_out, []() {
            return !write_buffer->empty() || written_runs == runs_count;
        });
        if (written_runs == runs_count) {
            break;
        }
        // enter critical section
        DEBUG_COUT("[WRITER] Entering critical section.\n");

        is_writing = true;
        ++written_runs;
        std::string filename = filename_prefix + std::to_string(written_runs);
        output_fs.open(filename, std::ios::out | std::ios::binary);
        length_per_run.push(
            std::make_pair(written_runs, write_buffer->getSize()));
        DEBUG_COUT("[WRITER] Writing %d items as run #%d.\n",
                   write_buffer->getSize(), written_runs);
        DEBUG_COUT("[WRITER] output filename: %s\n", filename.c_str());
        T cur_data;
        while (write_buffer->getNext(cur_data)) {
            output_fs.write(reinterpret_cast<char*>(&cur_data), sizeof(T));
            disk_write_count++;
        }
        output_fs.close();

        is_writing = false;
        lk_out.unlock();  // exit critical section

        sort_cond.notify_one();

        if (written_runs == runs_count) {
            DEBUG_COUT("[WRITER] written_runs == runs_count, finish.\n");
            printf("Writer thread: finished writing!\n");
            break;
        }
    }
}

int main() {
    CLOCK_TIK;
    std::cout.setf(std::ios::unitbuf);  // no buffer for std::cout

    std::string input_name = "data_chunk_256MB";
    std::string output_prefix = "data_chunk_256MB_run_";

    disk_read_count = disk_write_count = 0;

    std::thread reader_thread(reader_function<uint32_t>, input_name.c_str());
    std::thread sort_thread(sort_function<uint32_t>);
    std::thread writer_thread(writer_function<uint32_t>, output_prefix.c_str());

    reader_thread.join();
    sort_thread.join();
    writer_thread.join();

    CLOCK_TOK;

    // calculate the optimal merging order
    /*
    Huffman<run_len_pair, decltype(length_per_run)> huffman{runs_count,
                                                            length_per_run};

    huffman.forward(2, true);
    */

    /**
     *  Example: write calculated huffman to the global variables
     *      with efficient move semantics
     *  runs_count = huffman.run_limit_;
     *  length_per_run = std::move(huffman.container_);
     */

    std::cout << "Disk read count: " << disk_read_count << std::endl;
    std::cout << "Disk write count: " << disk_write_count << std::endl;

#ifdef FINAL_CHECK
    /*
    for (int i = 1; i <= runs_count; i++) {
        std::string output_name = output_prefix + std::to_string(i);
        std::cout << output_name << " sorted? " << std::boolalpha
                  << is_sorted<uint32_t>(output_name) << std::endl;
    }
    std::cout << "Consistent? " << std::boolalpha
              << is_consistent<uint32_t>(input_name, output_prefix, 1,
                                         runs_count)
              << std::endl;
    */
    for (int i = 1; i <= runs_count; i++) {
        std::string output_name = output_prefix + std::to_string(i);
        assert(is_sorted<uint32_t>(output_name));
    }
    assert(is_consistent<uint32_t>(input_name, output_prefix, 1, runs_count));
#endif

    CLOCK_RESET;

    /* MERGE RUNS */
    LoserTree<uint32_t, 8, BLOCK_SIZE> losertree(output_prefix, runs_count,
                                                 length_per_run);
    losertree.pipeline();

    CLOCK_TOK;

    length_per_run = std::move(
        static_cast<decltype(length_per_run)>(losertree.huffman_.container_));
    runs_count = losertree.run_limit_;

    std::cout << "Back to main. We have runs_count = " << runs_count
              << std::endl;

    return 0;
}
