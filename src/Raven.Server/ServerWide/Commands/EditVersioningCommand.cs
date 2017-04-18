using System;
using Raven.Client.Server;
using Raven.Server.Documents.Versioning;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class EditVersioningCommand : UpdateDatabaseCommand
    {
        public VersioningConfiguration Configuration;

        public EditVersioningCommand() 
            : base(null)
        {
            // for deserialization
        }

        public EditVersioningCommand(VersioningConfiguration configuration, string databaseName)
            : base(databaseName)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord databaseRecord, long etag)
        {
            databaseRecord.VersioningConfiguration = Configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            throw new NotImplementedException();
        }
    }
}