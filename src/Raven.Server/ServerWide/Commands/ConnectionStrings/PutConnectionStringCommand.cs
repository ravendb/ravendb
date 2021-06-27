using System;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ConnectionStrings
{
    public abstract class PutConnectionStringCommand<T> : UpdateDatabaseCommand where T : ConnectionString
    {
        public T ConnectionString { get; protected set; }

        protected PutConnectionStringCommand()
        {
            // for deserialization
        }

        protected PutConnectionStringCommand(T connectionString, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            ConnectionString = connectionString;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ConnectionString)] = ConnectionString.ToJson();
        }
    }

    public class PutRavenConnectionStringCommand : PutConnectionStringCommand<RavenConnectionString>
    {
        protected PutRavenConnectionStringCommand()
        {
            // for deserialization
        }

        public PutRavenConnectionStringCommand(RavenConnectionString connectionString, string databaseName, string uniqueRequestId) : base(connectionString, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            if (ConnectionString.Name.StartsWith(ServerWideExternalReplication.RavenConnectionStringPrefix, StringComparison.OrdinalIgnoreCase))
            {
                var isNewConnectionString = record.RavenConnectionStrings.ContainsKey(ConnectionString.Name);
                throw new InvalidOperationException($"Can't {(isNewConnectionString ? "create" : "update")} connection string: '{ConnectionString.Name}'. " +
                                                          $"A regular (non server-wide) connection string name can't start with prefix '{ServerWideExternalReplication.RavenConnectionStringPrefix}'");
            }

            record.RavenConnectionStrings[ConnectionString.Name] = ConnectionString;
            record.ClusterState.LastRavenEtlsIndex = index;
        }
    }

    public class PutSqlConnectionStringCommand : PutConnectionStringCommand<SqlConnectionString>
    {
        protected PutSqlConnectionStringCommand()
        {
            // for deserialization
        }

        public PutSqlConnectionStringCommand(SqlConnectionString connectionString, string databaseName, string uniqueRequestId) : base(connectionString, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            record.SqlConnectionStrings[ConnectionString.Name] = ConnectionString;
            record.ClusterState.LastSqlEtlsIndex = index;
        }
    }

    public class PutOlapConnectionStringCommand : PutConnectionStringCommand<OlapConnectionString>
    {
        protected PutOlapConnectionStringCommand()
        {
            // for deserialization
        }

        public PutOlapConnectionStringCommand(OlapConnectionString connectionString, string databaseName, string uniqueRequestId) : base(connectionString, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            record.OlapConnectionStrings[ConnectionString.Name] = ConnectionString;
            record.ClusterState.LastOlapEtlsIndex = index;
        }
    }


}
