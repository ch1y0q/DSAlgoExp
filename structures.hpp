/**
 * @file structures.hpp
 * @author HUANG Qiyue
 * @brief
 * @version 0.1
 * @date 2021-11-05
 *
 * @copyright Copyright (c) 2021
 *
 */

#ifndef STRUCTURES_H
#define STRUCTURES_H

#include <deque>
#include <fstream>
#include <iostream>
#include <list>
#include <queue>
#include <unordered_map>

#include "LoserTree.hpp"
#include "defs.h"
#include "extern/MinMaxHeap.hpp"

/**
 * @brief Defines a generic 2-D matrix with metadata.
 *
 * @tparam T Type of variables stored in the matrix.
 */
template <class T>
struct matrix_t {
    /* metadata */
    /* MATRIX_METADATA in defs.h */
    uint32_t magic;
    uint32_t row;
    uint32_t col;
    uint32_t size_of_T;

    /* 2-D array */
    T** arr;
};

/**
 * @brief A buffer that takes in a file of matrix_t<T> as input,
 *        LRU as replacement algorithm,
 *        write-through as write policy,
 *        and records buffer miss count against total read count.
 *
 * @tparam T Type of variables stored in the matrix.
 */
template <class T>
class buffer_t {
   private:
    struct vector_t;
    struct cache_line;

   public:
    /* constructors */
    buffer_t(char const* filename, size_t _size, size_t _window = 1) {
        fs.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        capacity = _size;
        window = _window;
        read_count = 0;
        miss_count = 0;

        /* load metadata */
        // fs.read((char*) &magic, sizeof(uint32_t));
        fs.seekg(sizeof(uint32_t), std::ios::beg); /* magic */
        fs.read((char*)&row, sizeof(uint32_t));
        fs.read((char*)&col, sizeof(uint32_t));
        fs.read((char*)&size_of_T, sizeof(uint32_t));
    }

    /* destructor */
    ~buffer_t() { fs.close(); }

    /* operators */
    vector_t operator[](size_t i) {
        if (i >= row) {
            std::cerr << "Index exceeds upper bound." << std::endl;
            return vector_t(0, this);
        }
        return vector_t(i, this);
    }

    /* helpers */
    void assign(size_t i, size_t j, T val) {
        auto key = i * (col) + j;
        fs.seekp(MATRIX_ARR_OFFSET + key * sizeof(T), std::ios::beg);
        fs.write(reinterpret_cast<char*>(&val), sizeof(T));
        LRU_set(key, val);
    }

    T seekg_and_read(size_t arr_offset) {
        fs.seekg(MATRIX_ARR_OFFSET + arr_offset, std::ios::beg);
        T ret;
        fs.read((char*)&ret, sizeof(T));
        return ret;
    }

    /* LRU */
    void LRU_set(size_t key, T value) {
        // key not found
        if (cache_map.find(key) == cache_map.end()) {
            // if is full
            if (cache_list.size() == capacity) {
                cache_map.erase(cache_list.back().key);
                cache_list.pop_back();
            }
            cache_list.push_front(cache_line(key, value));
            cache_map[key] = cache_list.begin();
        } else {
            // update and make it the first
            cache_map[key]->value = value;
            cache_list.splice(cache_list.begin(), cache_list, cache_map[key]);
            cache_map[key] = cache_list.begin();
        }
    }

    T LRU_get(size_t i, size_t j) {
        // read_count++;
        auto key = i * (col) + j;
        if (cache_map.find(key) == cache_map.end()) {  // cache miss!
            miss_count++;
            auto ret = seekg_and_read(key * sizeof(T));
            LRU_set(key, ret);
            for (auto k = 2; k <= window; ++k) {  // pre-fetch
                key++;
                if (key >= row * col) break;
                auto v = seekg_and_read(key * sizeof(T));
                LRU_set(key, v);
            }
            return ret;
        } else {
            cache_list.splice(cache_list.begin(), cache_list, cache_map[key]);
            cache_map[key] = cache_list.begin();
            return cache_map[key]->value;
        }
    }

    /* buffer params */
    size_t capacity;
    size_t window;

    /* persistent */
    uint32_t row;
    uint32_t col;
    uint32_t size_of_T;

    /* volatile */
    std::list<cache_line> cache_list;
    std::unordered_map<size_t, typename std::list<cache_line>::iterator>
        cache_map;

    /* stat */
    uint32_t miss_count;
    uint32_t read_count;
    inline float miss_rate() { return 1.0f * miss_count / read_count; }
    inline void reset_counters() { miss_count = read_count = 0; }

   private:
    std::fstream fs;

    /* structures */
    struct cache_line {
        uint32_t key;
        T value;
        cache_line(uint32_t k, T v) : key(k), value(v) {}
    };

    /* proxy class */
    struct vector_t {
        size_t i;
        buffer_t<T>* parent;
        /* constructors */
        vector_t(size_t _i, buffer_t<T>* e) : i(_i), parent(e) {}

        /* operators */

        /* read only; use assgin() to write */
        const T operator[](size_t j) const {
            T ret;

            if (j >= parent->col) {
                std::cerr << "Index exceeds upper bound." << std::endl;
                ret = parent->seekg_and_read(i * (parent->col) * sizeof(T));
            } else {
                parent->read_count++;
                ret = parent->LRU_get(i, j);
            }
            return ret;
        }
    };
};

/* Phase 2 */

template <class T>
class TreeNode {
    friend std::ostream& operator<<(std::ostream& out, TreeNode<T>& node) {
        out << node.data;
        return out;
    }

   public:
    TreeNode() : LeftChild(nullptr), RightChild(nullptr), data((T)0) {}
    TreeNode(T inData)
        : LeftChild(nullptr), RightChild(nullptr), data(inData) {}
    TreeNode(TreeNode* lnodeptr, TreeNode* rnodeptr, T inData) : data(inData) {
        LeftChild = RightChild = nullptr;
        if (lnodeptr) {
            auto lchild = new TreeNode(lnodeptr);
            LeftChild = lchild;
        }
        if (rnodeptr) {
            auto rchild = new TreeNode(rnodeptr);
            RightChild = rchild;
        }
    }
    TreeNode(const TreeNode* inNode)  // 复制构造函数，深拷贝
    {
        data = inNode->data;
        LeftChild = RightChild = nullptr;
        if (inNode->LeftChild) {
            auto lchild = new TreeNode(inNode->LeftChild);
            LeftChild = lchild;
        }
        if (inNode->RightChild) {
            auto rchild = new TreeNode(inNode->RightChild);
            RightChild = rchild;
        }
    }
    TreeNode(const TreeNode& inNode)  // 复制构造函数，深拷贝
    {
        data = inNode.data;
        LeftChild = RightChild = nullptr;
        if (inNode.LeftChild) {
            auto lchild = new TreeNode(inNode.LeftChild);
            LeftChild = lchild;
        }
        if (inNode.RightChild) {
            auto rchild = new TreeNode(inNode.RightChild);
            RightChild = rchild;
        }
    }

   public:
    TreeNode* LeftChild;
    TreeNode* RightChild;
    T data;
};

template <class T>
class ext_qsort_t {
   public:
    ext_qsort_t(unsigned int ninput, unsigned int nsmall, unsigned int nlarge,
                unsigned int nmiddle)
        : n_input(ninput), n_small(nsmall), n_large(nlarge), n_middle(nmiddle) {
        input_buffer.reserve(n_input);
        small_group.reserve(n_small);
        large_group.reserve(n_large);
    }
    unsigned int n_input;   // capacity of input buffer, size / sizeof(T)
    unsigned int n_small;   // capacity of small group
    unsigned int n_large;   // capacity of large group
    unsigned int n_middle;  // capacity of middle group

    std::vector<T> input_buffer;
    std::vector<T> small_group;
    std::vector<T> large_group;
    MinMaxHeap<T> middle_group;  // TODO: replace with Double-ended PQ
};

/* Phase 3 */
template <class T>
struct HeapNode {
    T data;
    int index;
    HeapNode(T a, int b) : data(a), index(b) {}
    bool operator<(const HeapNode& rhs) const { return (data < rhs.data); }
    bool operator>(const HeapNode& rhs) const { return (data > rhs.data); }
};

/* Phase 4 */
template <class T, class container>
class GenericBuffer {
   public:
    GenericBuffer() = default;
    GenericBuffer(size_t max_size) : max_size_(max_size){};
    ~GenericBuffer() = default;
    virtual bool getNext(T& out) = 0;
    virtual bool peekNext(T& out) const = 0;
    virtual bool push(const T& in) = 0;
    virtual bool empty() const = 0;
    virtual void clear() = 0;
    virtual void pop() = 0;
    virtual bool full() const = 0;
    virtual size_t getSize() const = 0;

   protected:
    container container_;
    size_t max_size_;
};

template <class T>
class QueueBuffer : public GenericBuffer<T, std::queue<T>> {
   public:
    QueueBuffer(size_t max_size) : GenericBuffer<T, std::queue<T>>(max_size) {}
    QueueBuffer(size_t max_size, size_t idx)
        : GenericBuffer<T, std::queue<T>>(max_size), idx_(idx) {}
    ~QueueBuffer() = default;

    /* move ctor */
    QueueBuffer(QueueBuffer&& other) { *this = std::move(other); }

    /* move assignment */
    QueueBuffer& operator=(QueueBuffer&& other) {
        if (&other == this) {
            return *this;
        }
        std::swap(this->container_, other.container_);
        this->max_size_ = other.max_size_;
        other.max_size_ = 0;
        this->idx_ = other.idx_;
        other.idx_ = 0;
        return *this;
    }

    bool empty() const override { return this->container_.empty(); }
    size_t getSize() const override { return this->container_.size(); }

    T getNext() {
        T out = this->container_.front();
        this->container_.pop();
        return out;
    }

    bool peekBack(T& out) const {
        if (empty()) return false;
        out = this->container_.back();
        return true;
    }

    bool getNext(T& out) override {
        if (empty()) return false;
        out = this->container_.front();
        this->container_.pop();
        return true;
    }

    bool peekNext(T& out) const override {
        if (empty()) return false;
        out = this->container_.front();
        return true;
    }

    void pop() override { this->container_.pop(); }

    void clear() override {
        while (!empty()) pop();
    }

    bool push(const T& in) override {
        if (getSize() >= this->max_size_) {
            return false;
        }
        this->container_.push(in);
        return true;
    }

    bool full() const override { return getSize() >= this->max_size_; }

    size_t idx_ = 0;

   private:
    // inherited from GenericBuffer
};

/* Phase 5 */

/* for deciding the best merge order */
using run_len_pair = std::pair<size_t, size_t>;
using run_len_pairs = std::vector<std::pair<size_t, size_t>>;
using run_len_pq =
    std::priority_queue<run_len_pair, run_len_pairs,
                        std::function<bool(run_len_pair, run_len_pair)>>;

/* used only to calculate the optimal merging order */
template <class T, class container>
class Huffman {
   public:
    Huffman(size_t run_limit, container _container)
        : run_limit_(run_limit), container_(_container) {}

    /* move ctor */
    Huffman(Huffman&& other) { *this = std::move(other); }

    template <typename container__, typename std::enable_if<!std::is_reference<
                                        container__>::value>::type* = nullptr>
    Huffman(size_t run_limit, container__&& _container)
        : run_limit_(run_limit) {
        std::swap(this->container_, _container);
    }

    /* move assignment */
    Huffman& operator=(Huffman&& other) {
        if (&other == this) {
            return *this;
        }
        std::swap(this->container_, other.container_);
        this->run_limit_ = other.run_limit_;
        other.run_limit_ = 0;
        return *this;
    }

    /* remove n_ary items from container, sum into one and add it back,
     * repeat nstep times */
    std::vector<T> forward(size_t n_ary, size_t nstep, bool show = false,
                           std::ostream& out = std::cout) {
        std::vector<T> ret = {};
        for (size_t i = 0; i < nstep; ++i) {
            if (container_.size() <= 1) break;
            decltype(container_.top().second) sum = 0;
            size_t num_merge = std::min(n_ary, container_.size());
            for (size_t j = 0; j < num_merge; ++j) {
                auto t = container_.top();
                ret.push_back(t);
                sum += t.second;
                container_.pop();
                if (show) {
                    out << "(" << t.first << ", " << t.second << ")";
                    if (j < num_merge - 1) {
                        out << " + ";
                    }
                }
            }
            run_limit_++;
            container_.push(std::make_pair(run_limit_, sum));
            if (show) {
                out << " = (" << run_limit_ << ", " << sum << ")\n";
            }
        }
        return ret;
    }

    std::vector<T> forward(size_t n_ary, bool show = false,
                           std::ostream& out = std::cout) {
        return forward(n_ary, SIZE_MAX, show, out);
    }

    size_t run_limit_;
    container container_;
};

template <class T, class BufferType, size_t nway, size_t buffer_size>
class BufferQueue {
   public:
    template <class KeyType, size_t K, size_t BlockSize>
    friend class LoserTree;

    /* ctor and dtor */
    /* Resource Allocation Is Initialization */
    BufferQueue() : k_(nway), buffer_size_(buffer_size) {
        for (size_t i = 1; i <= 2 * k_; ++i) {
            free_buffers_.push(new BufferType{buffer_size_, i});
        }
    }

    ~BufferQueue() {
        /* delete buffers */
        while (!free_buffers_.empty()) {
            auto* ptr = free_buffers_.front();
            delete ptr;
            free_buffers_.pop();
        }
        for (size_t i = 0; i < nway; ++i) {
            while (!buffers_[i].empty()) {
                auto* ptr = buffers_[i].front();
                delete ptr;
                (buffers_[i]).pop();
            }
        }
    }

    /* operator for easy access */
    BufferType* operator[](size_t idx) { return this->buffers_[idx].front(); }

    const size_t k_;
    const size_t buffer_size_;

   protected:
    std::queue<BufferType*> buffers_[nway];
    std::queue<BufferType*> free_buffers_;
};

template <class T, class BufferType, size_t buffer_size>
class OutputBuffer {
   public:
    template <class KeyType, size_t K, size_t BlockSize>
    friend class LoserTree;

    OutputBuffer() {
        buffers_.push_back(BufferType{buffer_size, 0});
        buffers_.push_back(BufferType{buffer_size, 1});
    }

    ~OutputBuffer() = default;

    size_t activeOutputBuffer =
        0;  // the buffer used for LoserTree; the other writing to disk
    bool is_writing = false;

   protected:
    std::vector<BufferType> buffers_;
};

#endif