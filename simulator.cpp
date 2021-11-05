/**
 * @file simulator.cpp
 * @author HUANG Qiyue
 * @brief 
 * @version 0.1
 * @date 2021-11-05
 * 
 * @copyright Copyright (c) 2021
 * 
 */

#include <iostream>
#include <fstream>
#include <algorithm>
#include <random>
#include "structures.h"
#include "defs.h"

matrix_t<uint32_t>
generate_matrix(uint32_t r, uint32_t c)
{
    matrix_t<uint32_t> m32;

    /* metadata */
    m32.magic=MAGIC;
    m32.size_of_T=sizeof(uint32_t);
    m32.row=r;
    m32.col=c;

    auto arr = new uint32_t*[r];
    /* generate random number */
    std::mt19937 rng(std::random_device{}());
    for(auto i=0; i<r; ++i){
        arr[i]=new uint32_t[c];
        for(auto j=0;j<c;++j){
            arr[i][j]=rng();
        }
    }

    m32.arr=arr;
    return m32;
}

template <class T>
void
dump_matrix(matrix_t<T>& mat, const char* filename)
{
    /* Create file object and open file */
    std::fstream fs(filename, std::ios::out | std::ios::binary);
    if (!fs)
    {
        std::cerr << "Error opening file. Aborting.\n";
        return;
    }
    /* dump metadata into file */
    fs.write(reinterpret_cast<char *>(&mat), MATRIX_METADATA * sizeof(uint32_t));

    /* dump array data into file */
    for(auto i=0; i<mat.row; ++i){
        for(auto j=0;j<mat.col;++j){
            fs.write(reinterpret_cast<char *>(&(mat.arr[i][j])), sizeof(T));
        }
    }
    fs.close();
}

template <class T>
matrix_t<T>
load_matrix(const char* filename)
{
    matrix_t<T> mat={0};

    /* Create file object and open file */
    std::fstream fs(filename, std::ios::in | std::ios::binary);
    if (!fs)
    {
        std::cerr << "Error opening file. Aborting.\n";
        return mat;
    }
    /* read metadata from file */
    fs.read((char*) &mat.magic, sizeof(uint32_t));
    fs.read((char*) &mat.row, sizeof(uint32_t));
    fs.read((char*) &mat.col, sizeof(uint32_t));
    fs.read((char*) &mat.size_of_T, sizeof(uint32_t));

    /* initialize array */
    mat.arr = new uint32_t*[mat.row];
    for(auto i=0; i<mat.row; ++i){
        mat.arr[i]=new uint32_t[mat.col];
    }

    /* read array data from file */
    for(auto i=0; i<mat.row; ++i){
        for(auto j=0;j<mat.col;++j){
            fs.read((char*) &mat.arr[i][j], sizeof(T));
        }
    }

    fs.close();
    return mat;
}

#define LOOP(_a,_b,_c) for(size_t _a = 0; _a < N; ++_a){for(size_t _b = 0; _b < N; ++_b){for(size_t _c = 0; _c < N; ++_c){c.assign(i,k,c[i][k]+a[i][j]*b[j][k]);}}}
#define SHOW_STAT(_x) printf("==" #_x "==\nRead: %d, Miss: %d, Miss Ratio: %.3f\n", _x.read_count, _x.miss_count, _x.miss_rate());
#define SHOW_STATS SHOW_STAT(a) SHOW_STAT(b) SHOW_STAT(c)
#define RUN_SIM(_a, _b, _c) printf(#_a #_b #_c ":\n");LOOP(_a,_b, _c);SHOW_STATS;printf("\n\n");
void
run_simulation(size_t N, size_t capacity)
{
    auto ma = generate_matrix(N, N);
    auto mb = generate_matrix(N, N);
    auto mc = generate_matrix(N, N);
    dump_matrix(ma, "a.dat");
    dump_matrix(mb, "b.dat");
    dump_matrix(mc, "c.dat");

    buffer_t<uint32_t> a("a.dat", capacity);
    buffer_t<uint32_t> b("b.dat", capacity);
    buffer_t<uint32_t> c("c.dat", capacity);

    RUN_SIM(i,j,k);
    RUN_SIM(i,k,j);
    RUN_SIM(j,i,k);
    RUN_SIM(j,k,i);
    RUN_SIM(k,i,j);
    RUN_SIM(k,j,i);
}

template <class T>
void
free_matrix(matrix_t<T>& mat)
{
    for(auto i=0; i<mat.row; ++i){
        delete[] mat.arr[i];
    }

    delete[] mat.arr;
}

int
main()
{
    /*
    auto a = generate_matrix(5, 5);
    auto b = generate_matrix(5, 5);
    auto c = generate_matrix(5, 5);
    dump_matrix(a, "a.dat");
    dump_matrix(b, "b.dat");
    dump_matrix(c, "c.dat");
    free_matrix(a);
    auto d = load_matrix<uint32_t>("a.dat");
    dump_matrix(d, "d.dat");
    
    buffer_t<uint32_t> myBuffer("d.dat",2);
    
    printf("%x\n",myBuffer[0][1]);
    printf("%x\n",myBuffer[0][1]);
    printf("%x\n",myBuffer[0][2]);
    printf("%x\n",myBuffer[0][2]);
    printf("%x\n",myBuffer[0][3]);
    printf("%x\n",myBuffer[0][1]);
    printf("%x\n",myBuffer[0][2]);
    printf("%x\n",myBuffer[0][3]);
    myBuffer[0][1]=1;   // myBuffer[0][0]=123 does NOT modifies the file!
    myBuffer.assign(0,1,1);
    printf("%x\n",myBuffer[0][1]);
    printf("%x\n",myBuffer[0][1]);
    printf("%x\n",myBuffer[0][2]);
    printf("%x\n",myBuffer[0][2]);
    printf("%x\n",myBuffer[0][3]);
    printf("%x\n",myBuffer[0][1]);
    printf("%x\n",myBuffer[0][2]);
    printf("%x\n",myBuffer[0][3]);
    printf("Read: %d, Miss: %d, Miss Ratio: %.3f\n", 
        myBuffer.read_count, myBuffer.miss_count, myBuffer.miss_rate());
        */

    run_simulation(5,10);
    std::cout<<"done\n";
    return 0;
}