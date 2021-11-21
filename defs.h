/**
 * @file defs.h
 * @author HUANG Qiyue
 * @brief 
 * @version 0.1
 * @date 2021-11-05
 * 
 * @copyright Copyright (c) 2021
 * 
 */

#ifndef DEFS_H
#define DEFS_H

/* Phase 1 */
#define MAGIC 0x114514
#define MATRIX_METADATA 4
#define MATRIX_ARR_OFFSET (MATRIX_METADATA*sizeof(uint32_t))

/* Phase 2 */
#define INPUT_BUFFER_PROPORTION 0.2
#define SMALL_GROUP_PROPORTION 0.3
#define LARGE_GROUP_PROPORTION 0.3

#define DUMPED_SMALL_SUFFIX ".s"
#define DUMPED_LARGE_SUFFIX ".l"

/* Phase 3 */
#define DUMPED_RUN_PREFIX "run_"

#endif