using System;
using System.Data.Common;

namespace Tests.Infrastructure.ConnectionString
{
    public abstract class SqlConnectionString<T> where T : DbConnection, new()
    {
        private readonly string _environmentVariable;
        private Lazy<string> ConnectionString { get; }

        protected string AdditionFlags { private get; set; } = string.Empty;

        public Lazy<string> VerifiedConnectionString { get; }

        private readonly Lazy<bool> _canConnect;

        public bool CanConnect => _canConnect.Value;

        protected SqlConnectionString(string environmentVariable)
        {
            _environmentVariable = environmentVariable;

            ConnectionString = new Lazy<string>(() =>
            {
                var connectionString = Environment.GetEnvironmentVariable(environmentVariable);
                return string.IsNullOrEmpty(connectionString)
                    ? string.Empty
                    : string.Join(';', connectionString, AdditionFlags);
            });

            VerifiedConnectionString = new Lazy<string>(() =>
            {
                var connectionString = ConnectionString.Value;
                return VerifiedConnectionStringFactory(connectionString);
            });

            _canConnect = new Lazy<bool>(CanConnectInternal);
        }

        private bool CanConnectInternal()
        {
            try
            {
                var connectionString = ConnectionString.Value;
                if (string.IsNullOrEmpty(connectionString))
                    return false;

                VerifiedConnectionStringFactory(connectionString);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        protected virtual string VerifiedConnectionStringFactory(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new InvalidOperationException($"Environment variable {_environmentVariable} is empty");

            connectionString = string.Join(";", connectionString, $"{TimeOutParameter}=3");

            try
            {
                using (var dbConnection = new T())
                {
                    dbConnection.ConnectionString = connectionString;
                    dbConnection.Open();
                }

                return ConnectionString.Value;
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Can't connect to {nameof(T)}. Connection string is {connectionString}", e);
            }
        }

        protected virtual string TimeOutParameter => "connection timeout";
    }
}
