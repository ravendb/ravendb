using MySqlConnector; // todo: check if it's ok to replace that import

namespace Tests.Infrastructure.ConnectionString
{
    public class MySqlConnectionString : SqlConnectionString<MySqlConnection>
    {
        private static MySqlConnectionString _instance;
        public static MySqlConnectionString Instance => _instance ??= new MySqlConnectionString();

        private MySqlConnectionString() : base("RAVEN_MYSQL_CONNECTION_STRING")
        {
        }
    }
}
