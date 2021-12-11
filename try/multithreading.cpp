#include <iostream>
#include <thread>

using namespace std;
volatile int t = 50;
int main() {
    std::thread reader_thread([]() { while(--t>0)cout << t<<"I'm a reader!\n"; });
    reader_thread.join();

    std::thread sort_thread([]() { while(--t>0)cout << t<<"I'm a sorter!\n"; });
    sort_thread.join();

    std::thread writer_thread([]() { while(--t>0)cout <<t<< "I'm a writer!\n"; });
    writer_thread.join();
}