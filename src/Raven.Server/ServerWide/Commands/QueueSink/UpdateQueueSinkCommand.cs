using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.QueueSink
{
    public class UpdateQueueSinkCommand : UpdateDatabaseCommand
    {
        public long TaskId { get; protected set; }

        public QueueSinkConfiguration Configuration { get; protected set; }
        
        public UpdateQueueSinkCommand()
        {
            // for deserialization
        }

        public UpdateQueueSinkCommand(long taskId, QueueSinkConfiguration configuration, string databaseName,
            string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            TaskId = taskId;
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.QueueSink, DatabaseName, null).UpdateDatabaseRecord(
                record, etag);
            new AddQueueSinkCommand(Configuration, DatabaseName, null).UpdateDatabaseRecord(record, etag);
        }
        
        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TaskId;
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
