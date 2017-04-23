using System;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Server.Documents.Versioning;
using Raven.Server.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class EditVersioningCommand : UpdateDatabaseCommand
    {
        // todo: change this back to VersioningConfiguration 
        public BlittableJsonReaderObject Configuration;

        // for deserialization
        public EditVersioningCommand() : base(null){}

        public EditVersioningCommand(BlittableJsonReaderObject configuration, string databaseName)
            : base(databaseName)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord databaseRecord, long etag)
        {
            databaseRecord.VersioningConfiguration = 
                JsonDeserializationServer.VersioningConfiguration(Configuration);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = Configuration;
        }
    }
}