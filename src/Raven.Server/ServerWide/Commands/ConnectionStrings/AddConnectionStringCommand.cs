using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ConnectionStrings
{
    public abstract class AddConnectionStringCommand<T> : UpdateDatabaseCommand where T : ConnectionString
    {
        public T ConnectionString { get; protected set; }

        protected AddConnectionStringCommand() : base(null)
        {
            // for deserialization
        }

        protected AddConnectionStringCommand(T connectionString, string databaseName) : base(databaseName)
        {
            ConnectionString = connectionString;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ConnectionString)] = TypeConverter.ToBlittableSupportedType(ConnectionString);
        }
    }

    public class AddRavenConnectionString : AddConnectionStringCommand<RavenConnectionString>
    {
        protected AddRavenConnectionString()
        {
            // for deserialization
        }

        public AddRavenConnectionString(RavenConnectionString connectionString, string databaseName) : base(connectionString, databaseName)
        {
            
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.RavenConnectionStrings[ConnectionString.Name] = ConnectionString;
            return null;
        }
    }

    public class AddSqlConnectionString : AddConnectionStringCommand<SqlConnectionString>
    {
        protected AddSqlConnectionString()
        {
            // for deserialization
        }

        public AddSqlConnectionString(SqlConnectionString connectionString, string databaseName) : base(connectionString, databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.SqlConnectionStrings[ConnectionString.Name] = ConnectionString;
            return null;
        }
    }
}