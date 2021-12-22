/**
 * @file LoserTree.hpp
 * @author HUANG Qiyue
 * @brief
 * @version 0.1
 * @date 2021-12-19
 *
 * @copyright Copyright (c) 2021
 *
 */

#ifndef LoserTree_hpp
#define LoserTree_hpp

#include <assert.h>
#include <limits.h>
#include <stdio.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "structures.hpp"
#include "utils/stats.hpp"
#include "utils/validation.hpp"

//#define LT_VALIDATION_ENABLED
//#define LT_DEBUG_COUT_ENABLED
#ifdef LT_DEBUG_COUT_ENABLED
#define LT_DEBUG_COUT(msg...) \
    printf(msg);              \
    fflush(stdout);
#else
#define LT_DEBUG_COUT(msg...)
#endif

/**
 * @brief
 *
 * @tparam KeyType Type of data field of each ndoe
 * @tparam K external nodes
 * @tparam BlockSize Number of data a buffer can accommodate
 */
template <class KeyType, size_t K, size_t BlockSize>
class LoserTree {
   public:
    LoserTree(std::string &run_prefix, size_t run_limit,
              run_len_pq &_run_len_pq)
        : run_prefix_(run_prefix),
          run_limit_(run_limit),
          huffman_(Huffman<run_len_pair, run_len_pq>{run_limit_,
                                                     std::move(_run_len_pq)}) {
        cleanup();
    }

    ~LoserTree() = default;

    /* cleanup */
    void cleanup() {
        for (size_t i = 0; i < K; ++i) {
            external_[i].key = 0;
            external_[i].nodeType = NodeType::MINKEY;
            is_reading_[i] = false;
            fss[i].close();
            bool flag = false;
            if (!input_.buffers_[i].empty()) {
                flag = true;
                LT_DEBUG_COUT(
                    "[CLEANUP] Buffer for node #%u is not empty with size "
                    "%u.\n",
                    i, input_.buffers_[i].size());
                while (!input_.buffers_[i].empty()) {
                    auto buf = input_.buffers_[i].front();
                    buf->clear();
                    auto idx = buf->idx_;
                    input_.free_buffers_.push(buf);
                    input_.buffers_[i].pop();
                    LT_DEBUG_COUT("%u ", idx);
                }
                LT_DEBUG_COUT("\n");
            }
            assert(!flag);
        }
        assert(input_.free_buffers_.size() == 2 * K);
        assert(output_.buffers_[0].empty());
        assert(output_.buffers_[1].empty());
        output_.is_writing = false;
        output_.activeOutputBuffer = 0;
    }

    /* the complete pipeline */
    void pipeline() {
        while (huffman_.container_.size() > 1) {
            do_work();

            // validation
            std::string output_filename =
                run_prefix_ + std::to_string(run_limit_);
            /* output validation
                std::cout << output_filename << " sorted? " << std::boolalpha
                          << is_sorted<KeyType>(output_filename) << std::endl;
                std::cout << "Consistency? " << std::boolalpha
                          << is_consistent<KeyType>(run_prefix_, runs_to_merge,
                                                    run_limit_)
                          << std::endl;
            */

#ifdef LT_VALIDATION_ENABLED
            assert(is_sorted<KeyType>(output_filename));
            assert(
                is_consistent<KeyType>(run_prefix_, runs_to_merge, run_limit_));
#endif

            // remove old runs
            remove_runs(runs_to_merge);

            // reset member variables
            cleanup();
        }
    }

    /* functions */
    void remove_runs(run_len_pairs run_len) {
        for (auto i : run_len) {
            std::string filename = run_prefix_ + std::to_string(i.first);
            remove(filename.c_str());
        }
    }

    void K_Merge() {
        LT_DEBUG_COUT("[K_MERGER] K_Merge at your service.\n");
        for (size_t i = 0; i < effective_K; ++i) {
            bool res = (input_.buffers_[i].front())->getNext(external_[i].key);
            assert(res);
            external_[i].nodeType = NodeType::DATAKEY;
        }
        for (size_t i = effective_K; i < K; ++i) {
            external_[i].nodeType = NodeType::MAXKEY;
        }
        LT_DEBUG_COUT("[K_MERGER] Create Loser Tree.\n");
        createLoserTree();
        LT_DEBUG_COUT("[K_MERGER] Finish creating Loser Tree.\n");
        while (external_[tree_[0]].nodeType != NodeType::MAXKEY) {
            size_t q = tree_[0];  // q is an external node index
            (output_.buffers_[output_.activeOutputBuffer])
                .push(
                    external_[q].key);  // fetch smallest element of loser tree
            LT_DEBUG_COUT("[K_MERGER] Push %u to output buffer #%u.\n",
                          external_[q].key, output_.activeOutputBuffer);
            bool res = (input_.buffers_[q].front())
                           ->getNext(external_[q].key);  // fetch next element
                                                         // from input buffer
            if (!res) {
                /* use next buffer! */
                LT_DEBUG_COUT("[K_MERGER] Use next buffer for node #%u.\n", q);
                std::unique_lock<std::mutex> lk(mut_free_buffers);
                input_.free_buffers_.push(input_.buffers_[q].front());
                input_.buffers_[q].pop();
                lk.unlock();
                LT_DEBUG_COUT("[K_MERGER] Notify free_buffers_cond.\n");
                free_buffers_cond
                    .notify_one();  // notify there is new free buffer
                if (input_.buffers_[q]
                        .empty()) {  // no new buffer in the buffer queue
                    if (fss[q].eof() && !is_reading_[q]) {
                        LT_DEBUG_COUT("[K_MERGER] Node #%u reached EOF.\n", q);
                        external_[q].nodeType = NodeType::MAXKEY;
                    } else {
                        LT_DEBUG_COUT(
                            "[K_MERGER] Waiting for next buffer of node "
                            "#%u.\n",
                            q);
                        std::unique_lock<std::mutex> lk(mut_is_reading);
                        in_lt.wait(lk, [=]() {
                            return !is_reading_[q] &&
                                   !input_.buffers_[q].empty();
                        });
                        lk.unlock();
                        assert(
                            !input_.buffers_[q].empty());  // shouldn't be empty
                        bool new_res = (input_.buffers_[q].front())
                                           ->getNext(external_[q].key);
                        assert(new_res);  // should be successful
                    }
                } else {
                    bool new_res =
                        (input_.buffers_[q].front())->getNext(external_[q].key);
                    if (!new_res) {  // no elements read by reader
                        assert(fss[q].eof());
                        LT_DEBUG_COUT("[K_MERGER] Node #%u reached EOF.\n", q);
                        std::unique_lock<std::mutex> lk(mut_free_buffers);
                        input_.free_buffers_.push(input_.buffers_[q].front());
                        input_.buffers_[q].pop();
                        lk.unlock();
                        external_[q].nodeType = NodeType::MAXKEY;
                    }
                }
            }  // end of if

            if (output_.buffers_[output_.activeOutputBuffer].full()) {
                LT_DEBUG_COUT("[K_MERGER] activeOutputBuffer is full.\n");
                std::unique_lock<std::mutex> lk(mut_is_writing);
                out_lt.wait(lk, [&]() {
                    return !output_.is_writing &&
                           output_.buffers_[1 - output_.activeOutputBuffer]
                               .empty();
                });
                LT_DEBUG_COUT(
                    "[K_MERGER] Finish waiting for output writing.\n");
                lk.unlock();
                auto write_thread = std::thread([=]() {
                    return write_function(output_.activeOutputBuffer,
                                          run_limit_);
                });
                write_thread.join();
                output_.activeOutputBuffer = 1 - output_.activeOutputBuffer;
            }
            adjust(q);
        }  // end of while

        /* remainders */
        LT_DEBUG_COUT("[K_MERGER] Dumping remainders.\n");
        if (!output_.buffers_[output_.activeOutputBuffer].empty()) {
            std::unique_lock<std::mutex> lk(mut_is_writing);
            out_lt.wait(lk, [&]() { return !output_.is_writing; });
            LT_DEBUG_COUT("[K_MERGER] Finish waiting for output writing.\n");
            lk.unlock();
            auto write_thread = std::thread([=]() {
                return write_function(output_.activeOutputBuffer, run_limit_);
            });
            write_thread.join();
            output_.activeOutputBuffer = 1 - output_.activeOutputBuffer;
        }
        LT_DEBUG_COUT("[K_MERGER] K_Merge done.\n");
    }

    // adjust the loser tree starting from external_[s]
    void adjust(size_t s) {
        size_t t;
        t = (s + K) / 2;
        while (t > 0) {
            if (external_[s] > external_[tree_[t]]) {
                auto tmp = s;
                s = tree_[t];
                tree_[t] = tmp;
            }
            t /= 2;
        }
        tree_[0] = s;
    }

    // initialize the loser tree
    void createLoserTree() {
        size_t i;
        external_[K].nodeType = NodeType::MINKEY;
        for (i = 0; i < K; i++) {
            tree_[i] = K;
        }
        for (i = K - 1;; --i) {  // i is size_t; do not test i>=0
            adjust(i);
            if (i == 0) break;
        }
    }

    void read_function(size_t node_idx, QueueBuffer<KeyType> *qb) {
        LT_DEBUG_COUT(
            "[READER] Init: Buffer #%u reading for external node #%u for run "
            "#%u.\n",
            qb->idx_, node_idx, run_idx_[node_idx]);

        /* init read buffer */
        qb->clear();

        while (!fss[node_idx].eof()) {
            KeyType cur_data;
            fss[node_idx].read((char *)&cur_data, sizeof(KeyType));
            // disk_read_count++;
            if (!fss[node_idx].eof()) {  // cur_data is valid
                qb->push(cur_data);
            }
            if (qb->full()) {
                std::unique_lock<std::mutex> lk(mut_is_reading);
                is_reading_[node_idx] = false;
                qb->peekBack(cur_max_[node_idx]);
                lk.unlock();
                LT_DEBUG_COUT(
                    "[READER] buffer %u full when reading run #%u for node "
                    "#%u.\n",
                    qb->idx_, run_idx_[node_idx], node_idx);

                in_lt.notify_one();  // notify do_work thread
                return;
            }
        }

        /* reach EOF of the run */
        fss[node_idx].close();

        if (!qb->empty()) {  // remaining elements in the buffer
            std::unique_lock<std::mutex> lk(mut_is_reading);
            is_reading_[node_idx] = false;
            qb->peekBack(cur_max_[node_idx]);
            lk.unlock();
            LT_DEBUG_COUT(
                "[READER] %u remainders as run #%u, notifying in_lt.\n",
                qb->getSize(), run_idx_[node_idx]);
            in_lt.notify_one();
        }
    }

    void write_function(size_t out_buffer_idx, size_t run_idx) {
        LT_DEBUG_COUT("[WRITER] Writer at your service.\n");
        std::unique_lock<std::mutex> lk(mut_is_writing);
        output_.is_writing = true;
        lk.unlock();

        LT_DEBUG_COUT("[WRITER] Start to write output buffer #%u, run #%u.\n",
                      out_buffer_idx, run_idx);

        std::string filename = run_prefix_ + std::to_string(run_idx);
        std::fstream output_fs;
        output_fs.open(filename,
                       std::ios::out | std::ios::binary | std::ios::app);
        LT_DEBUG_COUT("[WRITER] Writing %u items as run #%u.\n",
                      output_.buffers_[out_buffer_idx].getSize(), run_idx);
        LT_DEBUG_COUT("[WRITER] Output filename: %s\n", filename.c_str());
        KeyType cur_data;
        while (output_.buffers_[out_buffer_idx].getNext(cur_data)) {
            output_fs.write(reinterpret_cast<char *>(&cur_data),
                            sizeof(KeyType));
            // disk_write_count++;
        }
        output_fs.close();

        lk.lock();
        output_.is_writing = false;
        out_lt.notify_one();
    }

    void buffer_feeder() {
        while (true) {
            // wait for a free reader buffer
            LT_DEBUG_COUT("[BUFFER_FEEDER] Waiting for a free buffer.\n");
            std::unique_lock<std::mutex> lk(mut_free_buffers);
            free_buffers_cond.wait(
                lk, [&]() { return !input_.free_buffers_.empty(); });

            // which run will exhaust first?
            size_t node_idx = 0;
            auto min_cur_max_ = cur_max_[0];
            for (size_t i = 0; i < effective_K; ++i) {
                if (!fss[i].eof() &&
                    (cur_max_[i] < min_cur_max_ || fss[node_idx].eof())) {
                    node_idx = i;
                    min_cur_max_ = cur_max_[i];
                }
            }
            LT_DEBUG_COUT("[BUFFER_FEEDER] Will read for node #%u.\n",
                          node_idx);

            // prepare to read
            if (!fss[node_idx].eof()) {
                auto *qb = input_.free_buffers_.front();
                input_.free_buffers_.pop();
                input_.buffers_[node_idx].push(qb);
                std::unique_lock<std::mutex> lk2(mut_is_reading);
                is_reading_[node_idx] = true;
                lk2.unlock();
                lk.unlock();

                LT_DEBUG_COUT(
                    "[BUFFER_FEEDER] Invoke reader to read external node #%u "
                    "with buffer #%u.\n",
                    node_idx, qb->idx_);
                read_function(node_idx, qb);
            }

            // determine if all runs have been read (i.e. time to break)
            bool flag = true;
            for (size_t i = 0; i < effective_K; ++i) {
                if (!fss[i].eof()) {
                    flag = false;
                    break;
                }
            }
            if (flag) {
                LT_DEBUG_COUT("[BUFFER_FEEDER] All runs reached EOF.\n");
                break;  // end of while(true)
            }
        }
    }

    void do_work() {
        LT_DEBUG_COUT("[DO_WORK]: do_work at your service.\n");
        std::vector<std::thread> in_threads;
        runs_to_merge = std::move(huffman_.forward(K, 1, false));
        run_limit_ = huffman_.run_limit_;
        /* bind each run to an fstream */
        effective_K = runs_to_merge.size();
        LT_DEBUG_COUT("[DO_WORK]: Effectiv_K = %u\n", effective_K);
        LT_DEBUG_COUT("[DO_WORK]: Start to init buffers.\n");
        /* initially fill buffers */
        size_t i = 0;
        for (i = 0; i < effective_K; ++i) {
            run_idx_[i] = runs_to_merge[i].first;  // save run index

            // open file for reading
            std::string filename =
                run_prefix_ + std::to_string(runs_to_merge[i].first);
            fss[i].open(filename, std::ios::in | std::ios::binary);

            if (!fss[i].good()) {
                std::cerr << "File " << filename << " not found." << std::endl;
                exit(-1);
            }
            fss[i].seekg(0, fss[i].end);
            ssize_t input_size = fss[i].tellg();
            fss[i].seekg(0, fss[i].beg);

#ifdef LT_DEBUG_COUT_ENABLED
            PRINT_SEPARATOR_START;
            std::cout << "Reading file " << filename << " !" << std::endl;
            std::cout << "Total size (in bytes): " << input_size << std::endl;
            std::cout << "Total items: " << input_size / sizeof(KeyType)
                      << std::endl;
            PRINT_SEPARATOR_END;
#endif

            std::unique_lock<std::mutex> lk(mut_free_buffers);
            std::unique_lock<std::mutex> lk2(mut_is_reading);
            QueueBuffer<KeyType> *qb = input_.free_buffers_.front();
            input_.free_buffers_.pop();
            input_.buffers_[i].push(qb);
            is_reading_[i] = true;
            lk.unlock();
            lk2.unlock();

            auto th = std::thread([=]() { return read_function(i, qb); });
            in_threads.push_back(std::move(th));
        }
        /* start all reader threads */
        for (auto &thread : in_threads) {
            thread.join();
        }
        LT_DEBUG_COUT("[DO_WORK] Waiting for initial reader threads.\n");
        std::unique_lock<std::mutex> lk(mut_is_reading);
        in_lt.wait(lk, [&]() {
            return std::count(is_reading_, is_reading_ + effective_K, false) ==
                   effective_K;
        });  // wait for all reader to finish
        lk.unlock();
        LT_DEBUG_COUT("[DO_WORK] Starting K_Merge.\n");
        auto k_merge_thread = std::thread([=]() { return K_Merge(); });
        auto buffer_feeder_thread =
            std::thread([=]() { return buffer_feeder(); });

        k_merge_thread.join();
        buffer_feeder_thread.join();

        LT_DEBUG_COUT("[DO_WORK] Waiting for all threads to finish.\n");
        LT_DEBUG_COUT("[DO_WORK] Closing all opened files.\n");
        for (size_t i = 0; i < effective_K; ++i) {
            fss[i].close();
        }
    }

    /* puclic members */
    size_t run_limit_;
    Huffman<run_len_pair, run_len_pq> huffman_;
    std::string run_prefix_;
    size_t effective_K;
    BufferQueue<KeyType, QueueBuffer<KeyType>, K, BlockSize> input_;
    OutputBuffer<KeyType, QueueBuffer<KeyType>, BlockSize> output_;
    run_len_pairs runs_to_merge;

    /* sub-structs */

    /* MINKEY: uninitialized, DATAKEY: valid data, MAXKEY: end of run */
    enum class NodeType { MINKEY = -1, DATAKEY = 0, MAXKEY = 1 };

    struct ExNode {  // external node
        KeyType key;
        NodeType nodeType;
        bool operator<(const ExNode &rhs) {
            return (nodeType == rhs.nodeType && key < rhs.key) ||
                   nodeType < rhs.nodeType;
        }
        bool operator==(const ExNode &rhs) {
            return nodeType == rhs.nodeType && key == rhs.key;
        }
        bool operator>(const ExNode &rhs) {
            return (nodeType == rhs.nodeType && key > rhs.key) ||
                   nodeType > rhs.nodeType;
        }
    };

   private:
    size_t tree_[K];  // loser tree, saves index of external nodes
    ExNode external_[K + 1];

    std::fstream fss[K];
    bool is_reading_[K] = {false};
    KeyType cur_max_[K];
    size_t run_idx_[K];

    /* for multi-threading */
    std::mutex mut_is_reading;
    std::mutex mut_is_writing;
    std::mutex mut_free_buffers;
    std::condition_variable lt_out;
    std::condition_variable out_lt;
    std::condition_variable in_lt;
    std::condition_variable free_buffers_cond;
};

#endif /* LoserTree_hpp */
