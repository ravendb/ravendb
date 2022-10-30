using System;
using System.Collections.Generic;
using System.Linq;
using Amqp.Framing;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Extensions;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json.Parsing;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.ServerWide.Commands
{
    public class PutDatabaseSettingsCommand : UpdateDatabaseCommand
    {
        public Dictionary<string, string> Settings;

        public PutDatabaseSettingsCommand()
        {
            // for deserialization
        }

        public PutDatabaseSettingsCommand(Dictionary<string, string> settings, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Settings = settings;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Settings)] = TypeConverter.ToBlittableSupportedType(Settings);
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Settings = Settings;
        }
    }
}
