#include <algorithm>
#include <fstream>
#include <iostream>
using namespace std;

int main() {
    /* open file */
    std::ofstream output;
    std::string filename = "data_chunk_1KB";
    output.open(filename.c_str(), std::ios::out | std::ios::binary);

    if (!output.good()) {
        std::cerr << "Unable to read file " << filename << " !" << std::endl;
        exit(-1);
    }

    for (uint32_t i = 0; i < 250; ++i) {
        output.write(reinterpret_cast<char*>(&i), sizeof(uint32_t));
    }
    return 0;
}