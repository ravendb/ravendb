using Snowflake.Data.Client;
namespace Tests.Infrastructure.ConnectionString;

public class SnowflakeConnectionString : SqlConnectionString<SnowflakeDbConnection>
{
    private static SnowflakeConnectionString _instance;
    public static SnowflakeConnectionString Instance => _instance ??= new SnowflakeConnectionString();

    private SnowflakeConnectionString() : base("RAVEN_SNOWFLAKE_CONNECTION_STRING")
    {
    }
}
