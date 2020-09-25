using System;
using System.Data.SqlClient;

namespace Tests.Infrastructure.ConnectionString
{
    public class MssqlConnectionString : SqlConnectionString<SqlConnection>
    {
        private const string EnvironmentVariable = "RAVEN_MSSQL_CONNECTION_STRING";

        public static readonly MssqlConnectionString Instance = new MssqlConnectionString();

        private MssqlConnectionString() : base(EnvironmentVariable)
        {
        }

        protected override string VerifiedConnectionStringFactor()
        {
            const string localConnectionString = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=3";
            if (TryConnect(localConnectionString, out var errorMessage))
                return localConnectionString;

            var remoteConnectionString = Environment.GetEnvironmentVariable(EnvironmentVariable);
            if (TryConnect(remoteConnectionString, out errorMessage))
                return remoteConnectionString;

            throw new InvalidOperationException($"Use a valid connection string. " +
                                                $"Connection string from environment variable {EnvironmentVariable} is \"{remoteConnectionString}\"" +
                                                $"Local connection string is {localConnectionString}" + 
                                                $"Error message is '{errorMessage}'");

            bool TryConnect(string connectionString, out string errorMessage)
            {
                errorMessage = null;

                if (string.IsNullOrWhiteSpace(connectionString))
                {
                    errorMessage = "Empty connection string.";
                    return false;
                }

                connectionString = string.Join(";", connectionString, $"{TimeOutParameter}=3");

                try
                {
                    using (var connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                    }

                    return true;
                }
                catch (Exception e)
                {
                    errorMessage = e.ToString();
                    return false;
                }
            }
        }
    }
}
