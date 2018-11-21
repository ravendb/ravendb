using System;
using Npgsql;

namespace Tests.Infrastructure
{
    public class NpgSqlTests
    {
        public const string ConnectionString = "Host=localhost;Port=5432;Username=postgres;Password=1234567890"; //todo: use a real connection string
        public static string LocalConnection = $"{ConnectionString};Pooling=false"; // have to use pooling=false, otherwise closed connections are kept IDLE on PostgreSQL server.
        public static string LocalConnectionWithTimeout = $"{LocalConnection};connection timeout=3";

        public static readonly Lazy<string> NpgSqlDatabaseConnection = new Lazy<string>(() =>
        {
            using (var con = new NpgsqlConnection(LocalConnection))
            {
                con.Open();
            }

            return LocalConnection;
        });
    }
}
