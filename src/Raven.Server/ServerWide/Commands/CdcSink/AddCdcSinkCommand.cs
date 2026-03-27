using System.Collections.Generic;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.CdcSink
{
    public class AddCdcSinkCommand : UpdateDatabaseRecordFeaturesCommand
    {
        public CdcSinkConfiguration Configuration { get; protected set; }

        public AddCdcSinkCommand()
        {
            // for deserialization
        }

        public AddCdcSinkCommand(CdcSinkConfiguration configuration, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Add(ref record.CdcSinks, record, etag);
        }

        private void Add(ref List<CdcSinkConfiguration> cdcSinks, DatabaseRecord record, long etag)
        {
            if (string.IsNullOrEmpty(Configuration.Name))
                Configuration.Name = record.EnsureUniqueTaskName(Configuration.GetDefaultTaskName());

            EnsureTaskNameIsNotUsed(record, Configuration.Name);

            Configuration.TaskId = etag;

            cdcSinks ??= [];
            cdcSinks.Add(Configuration);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }

        public override bool Disabled => Configuration.Disabled;
    }
}
