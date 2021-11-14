/**
 * @file structures.h
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

#include <fstream>
#include <iostream>
#include <list>
#include <unordered_map>
#include <queue>
#include <deque>

#include "extern/MinMaxHeap.hpp"
#include "defs.h"
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
    T **arr;
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
    buffer_t(char const *filename, size_t _size, size_t _window = 1) {
        fs.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        capacity = _size;
        window = _window;
        read_count = 0;
        miss_count = 0;

        /* load metadata */
        // fs.read((char*) &magic, sizeof(uint32_t));
        fs.seekg(sizeof(uint32_t), std::ios::beg); /* magic */
        fs.read((char *)&row, sizeof(uint32_t));
        fs.read((char *)&col, sizeof(uint32_t));
        fs.read((char *)&size_of_T, sizeof(uint32_t));
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
        fs.write(reinterpret_cast<char *>(&val), sizeof(T));
        LRU_set(key, val);
    }

    T seekg_and_read(size_t arr_offset) {
        fs.seekg(MATRIX_ARR_OFFSET + arr_offset, std::ios::beg);
        T ret;
        fs.read((char *)&ret, sizeof(T));
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

    struct vector_t {
        size_t i;
        buffer_t<T> *parent;
        /* constructors */
        vector_t(size_t _i, buffer_t<T> *e) : i(_i), parent(e) {}

        /* operators */

        /* r-value */
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
	TreeNode() :LeftChild(nullptr), RightChild(nullptr), data((T)0) {}
	TreeNode(T inData) :LeftChild(nullptr), RightChild(nullptr), data(inData) {}
	TreeNode(TreeNode* lnodeptr, TreeNode* rnodeptr, T inData) : data(inData)
	{
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
	TreeNode(const TreeNode* inNode)			// 复制构造函数，深拷贝
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
	TreeNode(const TreeNode& inNode)			// 复制构造函数，深拷贝
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
    ext_qsort_t(unsigned int ninput, unsigned int nsmall,
                       unsigned int nlarge, unsigned int nmiddle)
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
#endif