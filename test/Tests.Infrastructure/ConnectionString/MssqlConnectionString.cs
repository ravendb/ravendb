using System;
using System.Data.SqlClient;

namespace Tests.Infrastructure.ConnectionString
{
    public class MssqlConnectionString: SqlConnectionString<SqlConnection>
    {
        private const string EnvironmentVariable = "RAVEN_MSSQL_CONNECTION_STRING";
        
        public static readonly MssqlConnectionString Instance = new MssqlConnectionString();

        private MssqlConnectionString(): base(EnvironmentVariable)
        {
        }

        protected override string VerifiedConnectionStringFactor()
        {
            const string localConnectionString = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=3";
            if (TryConnect(localConnectionString))
                return localConnectionString;

            var remoteConnectionString = Environment.GetEnvironmentVariable(EnvironmentVariable);
            if (TryConnect(remoteConnectionString))
                return remoteConnectionString;

            throw new InvalidOperationException($"Use a valid connection string. " +
                                                $"Connection string from environment variable {EnvironmentVariable} is \"{remoteConnectionString}\"" +
                                                $"Local connection string is {localConnectionString}");

            bool TryConnect(string connectionString)
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    return false;
                
                connectionString = string.Join(";", connectionString, $"{TimeOutParameter}=3");
                
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
