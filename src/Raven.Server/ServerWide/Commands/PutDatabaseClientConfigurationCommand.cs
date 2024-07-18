using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.ServerWide;
using Raven.Server.Extensions;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json.Parsing;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.ServerWide.Commands
{
    public class PutDatabaseClientConfigurationCommand : UpdateDatabaseCommand
    {

        public ClientConfiguration Configuration;

        public PutDatabaseClientConfigurationCommand()
        {
            // for deserialization
        }

        public PutDatabaseClientConfigurationCommand(ClientConfiguration client, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = client;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Client = Configuration;
            record.Client.Etag = etag;
        }
    }
}
