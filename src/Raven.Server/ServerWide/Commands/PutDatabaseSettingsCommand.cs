using System.Collections.Generic;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class PutDatabaseSettingsCommand : UpdateDatabaseCommand
    {
        public Dictionary<string, string> Configuration;

        public PutDatabaseSettingsCommand()
        {
            // for deserialization
        }

        public PutDatabaseSettingsCommand(Dictionary<string, string> settings, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = settings;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Settings = Configuration;
        }
    }
}
