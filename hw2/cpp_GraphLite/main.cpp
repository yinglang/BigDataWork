#include <iostream>
#include <limits>
int main() {
    double d = std::numeric_limits<double>::max();
    std::cout << d << std::endl;
    std::cout << (d == d-1) << std::endl;
    return 0;
}