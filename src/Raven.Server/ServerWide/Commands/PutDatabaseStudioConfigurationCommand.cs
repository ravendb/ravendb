using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.ServerWide;
using Raven.Server.Extensions;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json.Parsing;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.ServerWide.Commands
{
    public class PutDatabaseStudioConfigurationCommand : UpdateDatabaseCommand
    {
        public StudioConfiguration Configuration;

        public PutDatabaseStudioConfigurationCommand()
        {
            // for deserialization
        }

        public PutDatabaseStudioConfigurationCommand(StudioConfiguration studio, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = studio;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Studio = Configuration;
        }
    }
}
