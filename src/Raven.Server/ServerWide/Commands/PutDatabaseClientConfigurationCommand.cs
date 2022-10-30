using System;
using System.Collections.Generic;
using System.Linq;
using Amqp.Framing;
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
    public class PutDatabaseClientConfigurationCommand : UpdateDatabaseCommand
    {

        public ClientConfiguration Client;

        public PutDatabaseClientConfigurationCommand()
        {
            // for deserialization
        }

        public PutDatabaseClientConfigurationCommand(ClientConfiguration client, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Client = client;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Client)] = TypeConverter.ToBlittableSupportedType(Client);
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Client = Client;
            record.Client.Etag = etag;
        }
    }
}
