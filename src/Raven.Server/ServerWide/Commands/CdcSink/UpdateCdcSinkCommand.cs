using System;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.CdcSink
{
    public class UpdateCdcSinkCommand : UpdateDatabaseRecordFeaturesCommand
    {
        public long TaskId { get; protected set; }

        public CdcSinkConfiguration Configuration { get; protected set; }

        public UpdateCdcSinkCommand()
        {
            // for deserialization
        }

        public UpdateCdcSinkCommand(long taskId, CdcSinkConfiguration configuration, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            TaskId = taskId;
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var existing = record.CdcSinks?.Find(x => x.TaskId == TaskId);
            if (existing == null)
                throw new InvalidOperationException($"CDC Sink task with ID {TaskId} does not exist.");

            // Publication and slot names are immutable — they represent PostgreSQL resources
            // that were created when the task was first added. Users may omit them on update
            // (in which case we carry forward the existing values), but specifying different
            // names is an error because it would orphan the old resources.
            if (existing.Postgres != null)
            {
                if (Configuration.Postgres == null)
                {
                    Configuration.Postgres = existing.Postgres;
                }
                else
                {
                    if (Configuration.Postgres.PublicationName != null &&
                        Configuration.Postgres.PublicationName != existing.Postgres.PublicationName)
                    {
                        throw new InvalidOperationException(
                            $"Cannot change PublicationName from '{existing.Postgres.PublicationName}' to '{Configuration.Postgres.PublicationName}'. " +
                            "Publication names are immutable after task creation because they reference PostgreSQL resources.");
                    }

                    if (Configuration.Postgres.SlotName != null &&
                        Configuration.Postgres.SlotName != existing.Postgres.SlotName)
                    {
                        throw new InvalidOperationException(
                            $"Cannot change SlotName from '{existing.Postgres.SlotName}' to '{Configuration.Postgres.SlotName}'. " +
                            "Slot names are immutable after task creation because they reference PostgreSQL resources.");
                    }

                    Configuration.Postgres.PublicationName = existing.Postgres.PublicationName;
                    Configuration.Postgres.SlotName = existing.Postgres.SlotName;
                }
            }

            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.CdcSink, DatabaseName, null).UpdateDatabaseRecord(record, etag);
            new AddCdcSinkCommand(Configuration, DatabaseName, null).UpdateDatabaseRecord(record, etag);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TaskId;
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }

        public override bool Disabled => Configuration.Disabled;
    }
}
