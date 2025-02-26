#include <pqxx/pqxx>
#include <fstream>
#include <sstream>
#include <iostream>
#include <vector>
#include <string>
#include <stdexcept>
#include <chrono>

struct BlockData {
    std::string timestamp;           // Format: "YYYY-MM-DD HH:MM:SS"
    std::vector<std::string> hashes; // 31 hash fields
};

BlockData parse_block_csv_line(const std::string &line) {
    std::stringstream ss(line);
    std::string token;
    std::vector<std::string> tokens;
    while (std::getline(ss, token, ',')) {
        tokens.push_back(token);
    }
    if (tokens.size() != 32)
        throw std::runtime_error("Invalid CSV line: expected 32 fields, got " + std::to_string(tokens.size()));
    BlockData data;
    data.timestamp = tokens[0];
    data.hashes.assign(tokens.begin() + 1, tokens.end());
    return data;
}

int main() {
    try {
        pqxx::connection conn("postgresql://postgres:postgres@localhost/postgres");
        if (!conn.is_open()) {
            std::cerr << "Connection failed.\n";
            return 1;
        }

        // Create table "block" if doesn't exist
        {
            pqxx::work txn(conn);
            std::string create_table_sql = R"(
                CREATE TABLE IF NOT EXISTS block (
                    timestamp timestamp NOT NULL PRIMARY KEY,
                    hash_1  char(32) NOT NULL,
                    hash_2  char(32) NOT NULL,
                    hash_3  char(32) NOT NULL,
                    hash_4  char(32) NOT NULL,
                    hash_5  char(32) NOT NULL,
                    hash_6  char(32) NOT NULL,
                    hash_7  char(32) NOT NULL,
                    hash_8  char(32) NOT NULL,
                    hash_9  char(32) NOT NULL,
                    hash_10 char(32) NOT NULL,
                    hash_11 char(32) NOT NULL,
                    hash_12 char(32) NOT NULL,
                    hash_13 char(32) NOT NULL,
                    hash_14 char(32) NOT NULL,
                    hash_15 char(32) NOT NULL,
                    hash_16 char(32) NOT NULL,
                    hash_17 char(32) NOT NULL,
                    hash_18 char(32) NOT NULL,
                    hash_19 char(32) NOT NULL,
                    hash_20 char(32) NOT NULL,
                    hash_21 char(32) NOT NULL,
                    hash_22 char(32) NOT NULL,
                    hash_23 char(32) NOT NULL,
                    hash_24 char(32) NOT NULL,
                    hash_25 char(32) NOT NULL,
                    hash_26 char(32) NOT NULL,
                    hash_27 char(32) NOT NULL,
                    hash_28 char(32) NOT NULL,
                    hash_29 char(32) NOT NULL,
                    hash_30 char(32) NOT NULL,
                    hash_31 char(32) NOT NULL
                );
            )";
            txn.exec(create_table_sql);
            txn.commit();
        }

        // truncate table
        {
            pqxx::work txn_truncate(conn);
            txn_truncate.exec("TRUNCATE TABLE block");
            txn_truncate.commit();
        }

        // Prepare insert statement
        conn.prepare("insert_block",
            "INSERT INTO block (timestamp, hash_1, hash_2, hash_3, hash_4, hash_5, hash_6, hash_7, hash_8, hash_9, "
            "hash_10, hash_11, hash_12, hash_13, hash_14, hash_15, hash_16, hash_17, hash_18, hash_19, hash_20, "
            "hash_21, hash_22, hash_23, hash_24, hash_25, hash_26, hash_27, hash_28, hash_29, hash_30, hash_31) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, "
            "$11, $12, $13, $14, $15, $16, $17, $18, $19, $20, "
            "$21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32)"
        );

        // Open CSV file
        std::ifstream file("generated.csv");
        if (!file.is_open()) {
            std::cerr << "Could not open file generated.csv\n";
            return 1;
        }

        std::string line;
        // Read first line, also check for header
        if (std::getline(file, line)) {
            if (line.find("timestamp") == std::string::npos) {
                BlockData data = parse_block_csv_line(line);
                pqxx::work txn(conn);
                txn.exec_prepared("insert_block",
                    data.timestamp,
                    data.hashes[0],  data.hashes[1],  data.hashes[2],  data.hashes[3],
                    data.hashes[4],  data.hashes[5],  data.hashes[6],  data.hashes[7],
                    data.hashes[8],  data.hashes[9],  data.hashes[10], data.hashes[11],
                    data.hashes[12], data.hashes[13], data.hashes[14], data.hashes[15],
                    data.hashes[16], data.hashes[17], data.hashes[18], data.hashes[19],
                    data.hashes[20], data.hashes[21], data.hashes[22], data.hashes[23],
                    data.hashes[24], data.hashes[25], data.hashes[26], data.hashes[27],
                    data.hashes[28], data.hashes[29], data.hashes[30]
                );
                txn.commit();
            }
        }

        // Process remaining lines
        while (std::getline(file, line)) {
            if (line.empty()) continue;
            BlockData data = parse_block_csv_line(line);
            pqxx::work txn(conn);
            auto start = std::chrono::high_resolution_clock::now();
            {
                txn.exec_prepared("insert_block",
                                  data.timestamp,
                                  data.hashes[0], data.hashes[1], data.hashes[2], data.hashes[3],
                                  data.hashes[4], data.hashes[5], data.hashes[6], data.hashes[7],
                                  data.hashes[8], data.hashes[9], data.hashes[10], data.hashes[11],
                                  data.hashes[12], data.hashes[13], data.hashes[14], data.hashes[15],
                                  data.hashes[16], data.hashes[17], data.hashes[18], data.hashes[19],
                                  data.hashes[20], data.hashes[21], data.hashes[22], data.hashes[23],
                                  data.hashes[24], data.hashes[25], data.hashes[26], data.hashes[27],
                                  data.hashes[28], data.hashes[29], data.hashes[30]);
                txn.commit();
            }
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            std::cout << "Insert time for single record: " << duration.count() << " microseconds" << std::endl;
        }

        // Query latest block record
        {
            pqxx::work txn(conn);
            auto start = std::chrono::high_resolution_clock::now();
            pqxx::result r = txn.exec("SELECT * FROM block ORDER BY timestamp DESC LIMIT 1");
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            std::cout << "Read time: " << duration.count() << " microseconds" << std::endl;
            if (!r.empty()) {
                auto row = r[0];
                std::cout << "Latest Block:\nTimestamp: " << row["timestamp"].c_str() << "\n";
                for (int i = 1; i <= 31; i+=30) {
                    std::string col = "hash_" + std::to_string(i);
                    std::cout << col << ": " << row[col].c_str() << "\n";
                }
            }
            txn.commit();
        }

        conn.close();
    }
    catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}