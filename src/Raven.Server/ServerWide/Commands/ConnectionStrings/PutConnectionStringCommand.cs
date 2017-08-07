using Raven.Client.ServerWide;
using Raven.Client.ServerWide.ETL;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ConnectionStrings
{
    public abstract class PutConnectionStringCommand<T> : UpdateDatabaseCommand where T : ConnectionString
    {
        public T ConnectionString { get; protected set; }

        protected PutConnectionStringCommand() : base(null)
        {
            // for deserialization
        }

        protected PutConnectionStringCommand(T connectionString, string databaseName) : base(databaseName)
        {
            ConnectionString = connectionString;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ConnectionString)] = TypeConverter.ToBlittableSupportedType(ConnectionString);
        }
    }

    public class PutRavenConnectionString : PutConnectionStringCommand<RavenConnectionString>
    {
        protected PutRavenConnectionString()
        {
            // for deserialization
        }

        public PutRavenConnectionString(RavenConnectionString connectionString, string databaseName) : base(connectionString, databaseName)
        {
            
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.RavenConnectionStrings[ConnectionString.Name] = ConnectionString;
            return null;
        }
    }

    public class PutSqlConnectionString : PutConnectionStringCommand<SqlConnectionString>
    {
        protected PutSqlConnectionString()
        {
            // for deserialization
        }

        public PutSqlConnectionString(SqlConnectionString connectionString, string databaseName) : base(connectionString, databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.SqlConnectionStrings[ConnectionString.Name] = ConnectionString;
            return null;
        }
    }
}