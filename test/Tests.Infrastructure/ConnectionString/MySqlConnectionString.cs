using MySql.Data.MySqlClient;

namespace Tests.Infrastructure.ConnectionString
{
    public class MySqlConnectionString : SqlConnectionString<MySqlConnection>
    {
        private static MySqlConnectionString _instance;
        public static MySqlConnectionString Instance => _instance ?? (_instance = new MySqlConnectionString());

        private MySqlConnectionString() : base("RAVEN_MYSQL_CONNECTION_STRING")
        {
        }
    }
}
