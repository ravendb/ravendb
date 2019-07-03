using System;
using System.Data.Common;

namespace Tests.Infrastructure.ConnectionString
{
    public abstract class SqlConnectionString<T> where T : DbConnection, new()
    {
        private readonly string _environmentVariable;
        private Lazy<string> ConnectionString { get;}

        protected string AdditionFlags { private get; set; } = string.Empty;
        
        public Lazy<string> VerifiedConnectionString { get; }

        protected SqlConnectionString(string environmentVariable)
        {
            _environmentVariable = environmentVariable;
            VerifiedConnectionString = new Lazy<string>(VerifiedConnectionStringFactor);
            
            ConnectionString = new Lazy<string>(() =>
            {
                var connectionString = Environment.GetEnvironmentVariable(environmentVariable);
                return string.IsNullOrEmpty(connectionString) 
                    ? string.Empty
                    : string.Join(';', connectionString, AdditionFlags);
            });
        }

        public bool CanConnect()
        {
            try
            {
                VerifiedConnectionStringFactor();
                return true;
            }
            catch(Exception)
            {
                return false;
            }
        }

        protected virtual string VerifiedConnectionStringFactor()
        {
            if(string.IsNullOrEmpty(ConnectionString.Value))
                throw new InvalidOperationException($"Environment variable {_environmentVariable} is empty");

            var connectionString = string.Join(";", ConnectionString.Value, $"{TimeOutParameter}=3");

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
