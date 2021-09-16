using System;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;

namespace QueryExecutor
{
    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length < 2)
            {
                throw new Exception("Syntax: QueryExecutor connection-string query");
            }

            var connectionString = args[0];
            var query = args[1];

            while (true)
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    using (var command = new SqlCommand(query, connection))
                    {
                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var output = new StringBuilder();
                                for (var i = 0; i < reader.FieldCount; i++)
                                {
                                    output.Append($"{reader[i]},");
                                }
                                Console.WriteLine(output);
                            }
                        }
                    }
                }

                await Task.Delay(5000);
            }
        }
    }
}
