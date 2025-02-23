#define BOOST_HANA_CONFIG_ENABLE_STRING_UDL

#include <ozo/request.h>
#include <ozo/connection_info.h>
#include <ozo/shortcuts.h>
#include <boost/asio/io_context.hpp>
#include <iostream>
#include <vector>
#include <chrono>
#include <fstream>
#include <sstream>

namespace asio = boost::asio;

struct BitcoinData
{
    double timestamp;
    std::string source;
    std::string destination;
    int64_t satoshi;
};

BitcoinData parse_bitcoin_csv_line(const std::string &line)
{
    std::stringstream ss(line);
    BitcoinData data;
    std::string token;

    // Parse Timestamp
    std::getline(ss, token, ',');
    data.timestamp = std::stol(token);

    // source
    std::getline(ss, data.source, ',');

    // destination
    std::getline(ss, data.destination, ',');

    // satoshi | 1 BTC = 100M satoshis
    std::getline(ss, token, ',');
    data.satoshi = std::stod(token);
    return data;
}

// optional - cout overload to print the struct
std::ostream &operator<<(std::ostream &os, const BitcoinData &data)
{
    os << "timestamp: " << data.timestamp << ", "
       << "source: " << data.source << ", "
       << "destination: " << data.destination << ", ";
    return os;
}

int main()
{
    try
    {
        asio::io_context io;
        ozo::connection_info conn_info("host=localhost port=5432 dbname=postgres user=postgres password=postgres");

        // Create Bitcoin table with TimescaleDB hypertable
        auto create_table_query = ozo::make_query(
            "CREATE TABLE IF NOT EXISTS bitcoin_transactions ("
            "    timestamp BIGINT NOT NULL,"
            "    source VARCHAR(63) NOT NULL,"
            "    destination VARCHAR(63) NOT NULL,"
            "    satoshi BIGINT NOT NULL"
            ")");

        auto create_hypertable_query = ozo::make_query("SELECT create_hypertable('bitcoin_transactions', by_range('timestamp'), if_not_exists => TRUE, migrate_data => TRUE)");

        ozo::rows_of<std::string> table_creation_result;
        ozo::rows_of<std::string> hypertable_creation_result;

        // Execute table creation
        ozo::request(conn_info[io], create_table_query,
                     ozo::deadline(std::chrono::seconds(30)),
                     ozo::into(table_creation_result),
                     [&](ozo::error_code ec, auto conn)
                     {
                         if (!ec)
                         {
                             if (!table_creation_result.empty() && table_creation_result.size() > 0 && std::get<0>(table_creation_result[0]) == "CREATE TABLE")
                             {
                                 std::cout << "Table 'bitcoin_transactions' created successfully\n";

                                 // Create hypertable after table creation
                                 ozo::request(std::move(conn), create_hypertable_query,
                                              ozo::deadline(std::chrono::seconds(30)),
                                              ozo::into(hypertable_creation_result),
                                              [&hypertable_creation_result](ozo::error_code ec, auto conn)
                                              {
                                                  if (!ec)
                                                  {
                                                      if (!hypertable_creation_result.empty() && hypertable_creation_result.size() > 0 && std::get<0>(hypertable_creation_result[0]) == "t")
                                                      {
                                                          std::cout << "Hypertable 'bitcoin_transactions' created successfully\n";
                                                      }
                                                      else
                                                      {
                                                          std::cout << "Hypertable 'bitcoin_transactions' already exists, creation skipped\n";
                                                      }
                                                  }
                                                  else
                                                  {
                                                      std::cerr << "Hypertable creation error: " << ec.message() << '\n';
                                                  }
                                              });
                             }
                             else
                             {
                                 std::cout << "Table 'bitcoin_transactions' already exists, creation skipped\n";
                             }
                         }
                         else
                         {
                             std::cerr << "Table creation error: " << ec.message() << '\n';
                         }
                     });

        io.run();
        io.restart();

        // Load and parse Bitcoin data
        std::ifstream file("btc/2020-10_02.csv");
        std::vector<BitcoinData> batch;
        BitcoinData record;
        std::string line;

        // Skip header again
        std::getline(file, line);

        // Read just 1 line for single row
        if (std::getline(file, line))
        {
            record = parse_bitcoin_csv_line(line);
        }

        // Skip header
        std::getline(file, line);

        // Read up to max_records
        for (size_t i = 0; i < 50 && std::getline(file, line); ++i)
        {
            BitcoinData data = parse_bitcoin_csv_line(line);
            batch.push_back(data);
        }

        // Single insert function
        auto single_insert = [&](const BitcoinData &data)
        {
            auto query = ozo::make_query(
                "INSERT INTO bitcoin_transactions VALUES ($1, $2, $3, $4) ",
                data.timestamp,
                data.source,
                data.destination,
                data.satoshi);

            ozo::rows_of<> single_insert_res;

            ozo::request(conn_info[io], query,
                         ozo::deadline(std::chrono::seconds(30)),
                         ozo::into(single_insert_res),
                         [](ozo::error_code ec, auto conn)
                         {
                             if (ec)
                             {
                                 std::cerr << "Single insert error: " << ec.message() << '\n';
                             }
                         });
        };

        // Batch insert function
        auto batch_insert = [&](const std::vector<BitcoinData> &data_batch)
        {
            // Prepare the batch query
            auto batch_query = ozo::make_query(
                "INSERT INTO bitcoin_transactions VALUES ($1, $2, $3, $4)");

            // Prepare the batch parameters
            std::vector<std::tuple<std::time_t, std::string, std::string, int64_t>> batch_params;

            // Populate batch_params
            for (const auto &data : data_batch)
            {
                batch_params.emplace_back(data.timestamp, data.source, data.destination, data.satoshi);
            }

            // Execute the batch insert
            for (const auto &params : batch_params)
            {
                auto query = ozo::make_query(
                    "INSERT INTO bitcoin_transactions VALUES ($1, $2, $3, $4)",
                    std::get<0>(params),
                    std::get<1>(params),
                    std::get<2>(params),
                    std::get<3>(params));

                ozo::rows_of<> batch_insert_res;
                ozo::request(conn_info[io], query,
                             ozo::deadline(std::chrono::seconds(30)),
                             ozo::into(batch_insert_res),
                             [](ozo::error_code ec, auto conn)
                             {
                                 if (ec)
                                 {
                                     std::cerr << "x";
                                 }
                             });
            }
            std::cout << "\n";
        };

        // // Execute insert for single record | for single inserts
        // single_insert(record);

        // // Execute insert for batch
        // const size_t batch_size = 10;
        // for (size_t i = 0; i < batch.size(); i += batch_size)
        // {
        //     auto start = batch.begin() + i;
        //     auto end = (i + batch_size) < batch.size() ? start + batch_size : batch.end();
        //     std::vector<BitcoinData> batch_chunk(start, end);
        //     batch_insert(batch_chunk);
        // }

        // io.run();
        // std::cout << "All batches inserted\n";
        // io.restart();

        // Query functions
        auto single_read = [&]()
        {
            auto query = ozo::make_query("SELECT * FROM bitcoin_transactions ORDER BY timestamp DESC LIMIT 1");

            ozo::rows_of<double, std::string, std::string, int64_t> result;
            ozo::request(conn_info[io], query,
                         ozo::deadline(std::chrono::seconds(30)),
                         ozo::into(result),
                         [&result](ozo::error_code ec, auto conn)
                         {
                             if (!ec && !result.empty())
                             {
                                 const auto &row = result[0];
                                 std::cout << "\nLatest entry:\n"
                                           << "Time: " << std::get<0>(row) << "\t"
                                           << "Source wallet: " << std::get<1>(row) << "\t"
                                           << "Destination wallet: " << std::get<2>(row) << "\t"
                                           << "Satoshis: " << std::get<3>(row) << "\n";
                             }
                             else if (ec)
                             {
                                 std::cerr << "Single read error: " << ec.message() << '\n';
                             }
                         });
        };

        auto batch_read = [&](int limit)
        {
            auto query = ozo::make_query("SELECT * FROM bitcoin_transactions ORDER BY timestamp DESC LIMIT $1", limit);

            ozo::rows_of<double, std::string, std::string, int64_t> result;
            ozo::request(conn_info[io], query,
                         ozo::deadline(std::chrono::seconds(30)),
                         ozo::into(result),
                         [&result](ozo::error_code ec, auto conn)
                         {
                             if (!ec)
                             {
                                 std::cout << "\nBatch read results:\n";
                                 for (const auto &row : result)
                                 {
                                     std::cout << "Time: " << std::get<0>(row) << "\t"
                                               << "Source wallet: " << std::get<1>(row) << "\t"
                                               << "Destination wallet: " << std::get<2>(row) << "\t"
                                               << "Satoshis: " << std::get<3>(row) << "\n";
                                 }
                             }
                             else
                             {
                                 std::cerr << "Batch read error: " << ec.message() << '\n';
                             }
                         });
        };

        // Execute queries
        single_read();
        batch_read(5);
        io.run();
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
