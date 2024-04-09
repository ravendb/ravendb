using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using Elastic.Clients.Elasticsearch;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Commands.Revisions;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Schemas;
using Raven.Server.Documents.Sharding.Handlers.Processors.Stats;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Handlers.Admin;

public class AdminForbiddenUnusedIdsHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/get-forbidden-unused-ids", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
    public async Task GetForbiddenUnusedDatabaseIds()
    {
        using (var processor = new AdminForbiddenUnusedIdsHandlerProcessorForGetUnusedIds(this))
            await processor.ExecuteAsync();
    }
}
