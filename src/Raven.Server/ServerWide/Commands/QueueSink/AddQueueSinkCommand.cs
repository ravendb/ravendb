using System.Collections.Generic;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.QueueSink
{
    public class AddQueueSinkCommand: UpdateDatabaseCommand
    {
        public QueueSinkConfiguration Configuration { get; protected set; }
        
        public AddQueueSinkCommand()
        {
            // for deserialization
        }

        public AddQueueSinkCommand(QueueSinkConfiguration configuration, string databaseName, string uniqueRequestId) 
            : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Add(ref record.QueueSinks, record, etag);
        }

        private void Add(ref List<QueueSinkConfiguration> queueSinks, DatabaseRecord record, long etag)
        {
            if (string.IsNullOrEmpty(Configuration.Name))
            {
                Configuration.Name = record.EnsureUniqueTaskName(Configuration.GetDefaultTaskName());
            }

            EnsureTaskNameIsNotUsed(record, Configuration.Name);

            Configuration.TaskId = etag;

            if (queueSinks == null)
                queueSinks = new List<QueueSinkConfiguration>();

            queueSinks.Add(Configuration);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
