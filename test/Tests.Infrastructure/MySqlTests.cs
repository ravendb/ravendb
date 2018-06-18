using System;
using MySql.Data.MySqlClient;

namespace Tests.Infrastructure
{
    public class MySqlTests
    {
        public const string LocalConnection = @"server=127.0.0.1;uid=root;pwd=;sslMode=None";

        public static readonly Lazy<string> MySqlDatabaseConnection = new Lazy<string>(() =>
        {
            using (var con = new MySqlConnection(LocalConnection))
            {
                con.Open();
            }

            return LocalConnection;
        });
    }
}
