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

#include <iostream>
#include <unordered_map>
#include <list>
#include <fstream>

#include "defs.h"
/**
 * @brief Defines a generic 2-D matrix with metadata.
 * 
 * @tparam T Type of variables stored in the matrix.
 */
template <class T>
struct matrix_t
{
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
class buffer_t
{
private:
    struct vector_t;
    struct cache_line;
public:
    /* constructors */
    buffer_t(char const* filename, size_t _size)
    {
        fs.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        capacity = _size;
        read_count = 0;
        miss_count = 0;

        /* load metadata */
        //fs.read((char*) &magic, sizeof(uint32_t));
        fs.seekg(sizeof(uint32_t), std::ios::beg);/* magic */
        fs.read((char*) &row, sizeof(uint32_t));
        fs.read((char*) &col, sizeof(uint32_t));
        fs.read((char*) &size_of_T, sizeof(uint32_t));
    }

    /* operators */
    vector_t operator[](size_t i)
    {
        if( i >= row )
        {
            std::cerr << "Index exceeds upper bound." << std::endl;
            return vector_t(0, this);
        }
        return vector_t(i, this);
    }

    /* helpers */
    void assign(size_t i, size_t j, T val)
    {
        auto key = i*(col)+j;
        fs.seekp(MATRIX_ARR_OFFSET + key*sizeof(T), std::ios::beg);
        fs.write(reinterpret_cast<char *>(&val), sizeof(T));
        LRU_get(i, j);
    }

    T seekg_and_read(size_t arr_offset)
    {
        fs.seekg(MATRIX_ARR_OFFSET + arr_offset,std::ios::beg);
        T ret; 
        fs.read((char*) &ret, sizeof(T));
        return ret;
    }

    /* LRU */
    void LRU_set(size_t key, T value){
        // key not found
		if (cache_map.find(key) == cache_map.end())
		{
			// if is full
			if (cache_list.size() == capacity)
			{
				cache_map.erase(cache_list.back().key);
				cache_list.pop_back();
			}
			cache_list.push_front(cache_line(key,value));
			cache_map[key] = cache_list.begin();
		} 
		else
		{
			// update and make it the first
			cache_map[key]->value = value;
			cache_list.splice(cache_list.begin(),cache_list,cache_map[key]);
			cache_map[key] = cache_list.begin();
		}
	}

    T LRU_get(size_t i, size_t j)
    {
        read_count++;
        auto key = i*(col)+j;
        if (cache_map.find(key) == cache_map.end()){   // cache miss!
            miss_count++;
            auto ret = seekg_and_read(key*sizeof(T));
            LRU_set(key,ret);
            return ret;
        }
        else{
		    cache_list.splice(cache_list.begin(),cache_list,cache_map[key]);
		    cache_map[key] = cache_list.begin();
		    return cache_map[key]->value;
        }
    }
    
    /* buffer params */
    size_t capacity;

    /* persistent */
    uint32_t row;
    uint32_t col;
    uint32_t size_of_T;

    /* volatile */
	std::list<cache_line> cache_list;
	std::unordered_map<size_t, typename std::list<cache_line>::iterator> cache_map;

    /* stat */
    uint32_t miss_count;
    uint32_t read_count;
    inline float miss_rate() {return 1.0f * miss_count/read_count;}
    inline void reset_counters() {miss_count = read_count = 0;}
private:
    std::fstream fs;

    /* structures */
    struct cache_line{
		uint32_t key;
		T value;
		cache_line(uint32_t k,T v):key(k),value(v){}
	};

    struct vector_t{
        size_t i;
        buffer_t<T> *parent;
        /* constructors */
        vector_t(size_t _i, buffer_t<T> *e) : i(_i), parent(e){}

        /* operators */

        /* l-value */
        T& operator[](size_t j)
        {
            T* ret= new T;  // TODO: delete!

            if( j >= parent->col )
            {
                std::cerr << "Index exceeds upper bound." << std::endl;
                *ret =  parent->seekg_and_read(i*(parent->col)*sizeof(T));
            }
            else{
                *ret = parent->LRU_get(i, j);
            }
            return *ret;
        }
    };
};

#endif