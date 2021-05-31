using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ConnectionStrings
{
    public abstract class RemoveConnectionStringCommand<T> : UpdateDatabaseCommand where T : ConnectionString
    {
        public string ConnectionStringName { get; protected set; }

        protected RemoveConnectionStringCommand()
        {
            // for deserialization
        }

        protected RemoveConnectionStringCommand(string connectionStringName, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
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

        public RemoveRavenConnectionStringCommand(string connectionStringName, string databaseName, string uniqueRequestId) : base(connectionStringName, databaseName, uniqueRequestId)
        {
            
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            record.RavenConnectionStrings.Remove(ConnectionStringName);
            record.ClusterState.LastRavenEtlsIndex = index;
        }
    }

    public class RemoveSqlConnectionStringCommand : RemoveConnectionStringCommand<SqlConnectionString>
    {
        protected RemoveSqlConnectionStringCommand()
        {
            // for deserialization
        }

        public RemoveSqlConnectionStringCommand(string connectionStringName, string databaseName, string uniqueRequestId) : base(connectionStringName, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            record.SqlConnectionStrings.Remove(ConnectionStringName);
            record.ClusterState.LastSqlEtlsIndex = index;
        }
    }

    public class RemoveOlapConnectionStringCommand : RemoveConnectionStringCommand<OlapConnectionString>
    {
        protected RemoveOlapConnectionStringCommand()
        {
            // for deserialization
        }

        public RemoveOlapConnectionStringCommand(string connectionStringName, string databaseName, string uniqueRequestId) : base(connectionStringName, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            record.OlapConnectionStrings.Remove(ConnectionStringName);
            record.ClusterState.LastOlapEtlsIndex = index;
        }
    }
}
