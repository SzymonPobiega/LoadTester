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
                throw new Exception("Syntax: QueryExecutor interval connection-string query");
            }

            var interval = TimeSpan.Parse(args[0]);
            var connectionString = args[1];
            var query = args[2];

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
                                Console.WriteLine(DateTime.UtcNow + ": " + output);
                            }
                        }
                    }
                }

                await Task.Delay(interval);
            }
        }
    }
}
