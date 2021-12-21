/**
 * @file stats.hpp
 * @author HUANG Qiyue
 * @brief
 * @version 0.1
 * @date 2021-12-06
 *
 * @copyright Copyright (c) 2021
 *
 */

#ifndef STATS_H
#define STATS_H

#include <iostream>

#define PRINT_TIME_SO_FAR                                            \
    std::cout << "Elasped time: "                                    \
              << float(clock() - begin_time) / CLOCKS_PER_SEC * 1000 \
              << " msec." << std::endl;
#define PRINT_SEPARATOR_START \
    std::cout << "-------------------------------------------------------\n";
#define PRINT_SEPARATOR_END \
    std::cout               \
        << "-------------------------------------------------------\n\n\n";

#define CLOCK_TIK clock_t begin_time = clock();
#define CLOCK_RESET begin_time = clock();
#define CLOCK_TOK PRINT_TIME_SO_FAR

#endif