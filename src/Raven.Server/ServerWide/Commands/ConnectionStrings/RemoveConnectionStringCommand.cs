using Raven.Client.ServerWide;
using Raven.Client.ServerWide.ETL;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ConnectionStrings
{
    public abstract class RemoveConnectionStringCommand<T> : UpdateDatabaseCommand where T : ConnectionString
    {
        public string ConnectionStringName { get; protected set; }

        protected RemoveConnectionStringCommand() : base(null)
        {
            // for deserialization
        }

        protected RemoveConnectionStringCommand(string connectionStringName, string databaseName) : base(databaseName)
        {
            ConnectionStringName = connectionStringName;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ConnectionStringName)] = ConnectionStringName;
        }
    }

    public class RemoveRavenConnectionString : RemoveConnectionStringCommand<RavenConnectionString>
    {
        protected RemoveRavenConnectionString()
        {
            // for deserialization
        }

        public RemoveRavenConnectionString(string connectionStringName, string databaseName) : base(connectionStringName, databaseName)
        {
            
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.RavenConnectionStrings.Remove(ConnectionStringName);
            return null;
        }
    }

    public class RemoveSqlConnectionString : RemoveConnectionStringCommand<SqlConnectionString>
    {
        protected RemoveSqlConnectionString()
        {
            // for deserialization
        }

        public RemoveSqlConnectionString(string connectionStringName, string databaseName) : base(connectionStringName, databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.SqlConnectionStrings.Remove(ConnectionStringName);
            return null;
        }
    }
}