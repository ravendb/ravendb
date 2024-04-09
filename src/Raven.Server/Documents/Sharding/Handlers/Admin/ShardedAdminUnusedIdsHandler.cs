using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Sharding.Handlers.Admin;

internal class ShardedAdminUnusedIdsHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/get-forbidden-unused-ids", "GET")]
    public async Task GetForbiddenUnusedDatabaseIds()
    {
        using (var processor = new ShardedAdminForbiddenUnusedIdsHandlerProcessorForGetUnusedIds(this))
            await processor.ExecuteAsync();
    }
}

