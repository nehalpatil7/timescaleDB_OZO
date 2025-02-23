#define BOOST_HANA_CONFIG_ENABLE_STRING_UDL

#include <ozo/request.h>
#include <ozo/connection_info.h>
#include <ozo/shortcuts.h>
#include <boost/asio/io_context.hpp>
#include <iostream>
#include <chrono>

namespace asio = boost::asio;

int main()
{
    try
    {
        asio::io_context io;

        // Connection info
        ozo::connection_info conn_info("host=localhost port=5432 dbname=postgres user=postgres password=postgres");

        // Create table query using query builder
        auto create_table_query = ozo::make_query(
            "CREATE TABLE IF NOT EXISTS bitcoin_data ("
            "   time TIMESTAMPT NOT NULL,"
            "   sensor_id INTEGER,"
            "   temperature DOUBLE PRECISION"
            ")");

        // Create hypertable query
        auto create_hypertable_query = ozo::make_query("SELECT create_hypertable('sensor_data', by_range('time'), if_not_exists => TRUE, migrate_data => TRUE)");

        // Container for results
        ozo::rows_of<std::string> result1;
        ozo::rows_of<std::string> result2;

        // Execute create table query
        ozo::request(conn_info[io], create_table_query,
                     ozo::deadline(std::chrono::seconds(30)),
                     ozo::into(result1),
                     [&](ozo::error_code ec, auto conn)
                     {
                         if (!ec)
                         {
                             // Check the command tag
                             if (!result1.empty() && result1.size() > 0 && std::get<0>(result1[0]) == "CREATE TABLE")
                             {
                                 std::cout << "Table 'sensor_data' created successfully\n";

                                 // Execute create hypertable query
                                 ozo::request(std::move(conn), create_hypertable_query,
                                              ozo::deadline(std::chrono::seconds(30)),
                                              ozo::into(result2),
                                              [&result2](ozo::error_code ec, auto conn)
                                              {
                                                  if (!ec)
                                                  {
                                                      if (!result2.empty() && result2.size() > 0 && std::get<0>(result2[0]) == "t")
                                                      {
                                                          std::cout << "Hypertable 'sensor_data' created successfully\n";
                                                      }
                                                      else
                                                      {
                                                          std::cout << "Hypertable 'sensor_data' already exists, creation skipped\n";
                                                      }
                                                  }
                                                  else
                                                  {
                                                      std::cerr << "Error creating hypertable: " << ec.message() << '\n';
                                                  }
                                              });
                             }
                             else
                             {
                                 std::cout << "Table 'sensor_data' already exists, creation skipped\n";
                             }
                         }
                         else
                         {
                             std::cerr << "Error creating table: " << ec.message() << '\n';
                         }
                     });

        io.run();
        // Reset io context after first run
        io.restart();

        // Insert sample data
        auto insert_query = ozo::make_query(
            "INSERT INTO sensor_data (time, sensor_id, temperature) "
            "VALUES ($1, $2, $3) "
            "ON CONFLICT (sensor_id, time) DO NOTHING",
            std::chrono::system_clock::now(),
            1,
            25.5);

        ozo::rows_of<> result3;

        ozo::request(conn_info[io], insert_query,
                     ozo::deadline(std::chrono::seconds(30)),
                     ozo::into(result3),
                     [](ozo::error_code ec, auto conn)
                     {
                         if (!ec)
                         {
                             std::cout << "Data inserted successfully\n";
                         }
                         else
                         {
                             std::cerr << "Error inserting data: " << ec.message() << '\n';
                         }
                     });

        io.run();
        io.restart();

        // Query the inserted data
        auto select_query = ozo::make_query("SELECT time, sensor_id, temperature FROM sensor_data ORDER BY time DESC LIMIT 1");

        ozo::rows_of<std::chrono::system_clock::time_point, int32_t, double> result4;

        ozo::request(conn_info[io], select_query,
                     ozo::deadline(std::chrono::seconds(30)),
                     ozo::into(result4),
                     [&result4](ozo::error_code ec, auto conn)
                     {
                         if (!ec)
                         {
                             std::cout << "Query executed successfully\n";
                             for (const auto &row : result4)
                             {
                                 auto time = std::chrono::system_clock::to_time_t(std::get<0>(row));
                                 std::cout << "Time: " << std::ctime(&time)
                                           << "Sensor ID: " << std::get<1>(row)
                                           << ", Temperature: " << std::get<2>(row) << std::endl;
                             }
                         }
                         else
                         {
                             std::cerr << "Error querying data: " << ec.message() << '\n';
                             std::cerr << "Error category: " << ec.category().name() << '\n';
                             std::cerr << "Error value: " << ec.value() << '\n';
                         }
                     });

        io.run();
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}