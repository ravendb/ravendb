using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public abstract class UpdateEtlCommand<T, TConnectionString> : UpdateDatabaseCommand where T : EtlConfiguration<TConnectionString> where TConnectionString : ConnectionString
    {
        public long TaskId { get; protected set; }

        public T Configuration { get; protected set; }

        public EtlType EtlType { get; protected set; }

        protected UpdateEtlCommand()
        {
            // for deserialization
        }

        protected UpdateEtlCommand(long taskId, T configuration, EtlType type, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            TaskId = taskId;
            Configuration = configuration;
            EtlType = type;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TaskId;
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
            json[nameof(EtlType)] = EtlType;
        }
    }

    public class UpdateRavenEtlCommand : UpdateEtlCommand<RavenEtlConfiguration, RavenConnectionString>
    {
        public UpdateRavenEtlCommand()
        {
            // for deserialization
        }

        public UpdateRavenEtlCommand(long taskId, RavenEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(taskId, configuration, EtlType.Raven, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.RavenEtl, DatabaseName, null).UpdateDatabaseRecord(record, etag);
            new AddRavenEtlCommand(Configuration, DatabaseName, null).UpdateDatabaseRecord(record, etag);

        }
    }

    public class UpdateSqlEtlCommand : UpdateEtlCommand<SqlEtlConfiguration, SqlConnectionString>
    {
        public UpdateSqlEtlCommand()
        {
            // for deserialization
        }

        public UpdateSqlEtlCommand(long taskId, SqlEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(taskId, configuration, EtlType.Sql, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.SqlEtl, DatabaseName, null).UpdateDatabaseRecord(record, etag);
            new AddSqlEtlCommand(Configuration, DatabaseName, null).UpdateDatabaseRecord(record, etag);

        }
    }

    public class UpdateOlapEtlCommand : UpdateEtlCommand<OlapEtlConfiguration, OlapConnectionString>
    {
        public UpdateOlapEtlCommand()
        {
            // for deserialization
        }

        public UpdateOlapEtlCommand(long taskId, OlapEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(taskId, configuration, EtlType.Olap, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.OlapEtl, DatabaseName, null).UpdateDatabaseRecord(record, etag);
            new AddOlapEtlCommand(Configuration, DatabaseName, null).UpdateDatabaseRecord(record, etag);
        }
    }
}
