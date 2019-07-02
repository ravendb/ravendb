using System;
using System.Data.SqlClient;

namespace Tests.Infrastructure.ConnectionString
{
    public class MssqlConnectionString: SqlConnectionString<SqlConnection>
    {
        public static readonly MssqlConnectionString Instance = new MssqlConnectionString();

        private MssqlConnectionString(): base("RAVEN_MSSQL_CONNECTION_STRING")
        {
        }

        protected override string VerifiedConnectionStringFactor()
        {
            var cString = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=3";

            if (TryConnect(cString))
                return cString;

            cString = @"Data Source=ci1\sqlexpress;Integrated Security=SSPI;Connection Timeout=15";

            if (TryConnect(cString))
                return cString;

            cString = Environment.GetEnvironmentVariable("RAVEN_MSSQL_CONNECTION_STRING");

            if (TryConnect(cString))
                return cString;

            throw new InvalidOperationException("Use a valid connection");

            bool TryConnect(string connectionString)
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    return false;

                try
                {
                    using (var connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                    }

                    return true;
                }
                catch (Exception)
                {
                    return false;
                }
            }
        }
    }
}
