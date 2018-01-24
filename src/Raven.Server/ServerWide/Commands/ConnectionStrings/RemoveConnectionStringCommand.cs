using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
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

    public class RemoveRavenConnectionStringCommand : RemoveConnectionStringCommand<RavenConnectionString>
    {
        protected RemoveRavenConnectionStringCommand()
        {
            // for deserialization
        }

        public RemoveRavenConnectionStringCommand(string connectionStringName, string databaseName) : base(connectionStringName, databaseName)
        {
            
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.RavenConnectionStrings.Remove(ConnectionStringName);
            return null;
        }
    }

    public class RemoveSqlConnectionStringCommand : RemoveConnectionStringCommand<SqlConnectionString>
    {
        protected RemoveSqlConnectionStringCommand()
        {
            // for deserialization
        }

        public RemoveSqlConnectionStringCommand(string connectionStringName, string databaseName) : base(connectionStringName, databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.SqlConnectionStrings.Remove(ConnectionStringName);
            return null;
        }
    }
}
