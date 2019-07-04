using Npgsql;

namespace Tests.Infrastructure.ConnectionString
{
    public class NpgSqlConnectionString :SqlConnectionString<NpgsqlConnection>
    {
        private static NpgSqlConnectionString _instance;
        public static NpgSqlConnectionString Instance => _instance ?? (_instance = new NpgSqlConnectionString());
        
        private NpgSqlConnectionString() : base("RAVEN_NPGSQL_CONNECTION_STRING")
        {
            AdditionFlags = "Pooling=false"; // have to use pooling=false, otherwise closed connections are kept IDLE on PostgreSQL server.
        }
        
        protected override string TimeOutParameter => "Timeout";
    }
}
