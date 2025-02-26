#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <string>
#include <openssl/sha.h>

const size_t RECORD_COUNT = 1048576; // # records
const size_t COLUMNS = 32;           // total columns (1 timestamp + 31 hash)
const size_t FIELD_WIDTH = 32;       // each field = 32 bytes

// Helper function: convert binary data to a hex string.
std::string to_hex(const unsigned char* data, size_t length) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (size_t i = 0; i < length; i++) {
        oss << std::setw(2) << static_cast<int>(data[i]);
    }
    return oss.str();
}

int main() {
    std::ofstream out("generated.csv");
    if (!out) {
        std::cerr << "Error opening output file!" << std::endl;
        return 1;
    }

    out << "timestamp";
    // Loop to create headers for columns 1 to 31
    for (size_t col = 1; col < COLUMNS; col++)
    {
        out << ",hash_" << col;
    }
    out << "\n";

    for (size_t i = 0; i < RECORD_COUNT; i++) {
        std::ostringstream csvLine;

        // Column 0: Unique timestamp
        std::ostringstream tsStream;
        tsStream << std::setw(FIELD_WIDTH) << std::setfill('0') << std::hex << i;
        std::string timestamp = tsStream.str();
        csvLine << timestamp;

        // Columns 1 to 31: Compute SHA-256 hash (truncated to 32 hex characters)
        for (size_t col = 1; col < COLUMNS; col++) {
            csvLine << ",";
            std::string input = timestamp + std::to_string(col);

            unsigned char hash[SHA256_DIGEST_LENGTH];
            SHA256(reinterpret_cast<const unsigned char*>(input.data()), input.size(), hash);

            std::string hashHex = to_hex(hash, SHA256_DIGEST_LENGTH);
            csvLine << hashHex.substr(0, FIELD_WIDTH);
        }

        csvLine << "\n";
        out << csvLine.str();
    }

    out.close();
    std::cout << "Created " << RECORD_COUNT << " records in generated.csv" << std::endl;
    return 0;
}

// g++ generator.cpp -lcrypto -o generator