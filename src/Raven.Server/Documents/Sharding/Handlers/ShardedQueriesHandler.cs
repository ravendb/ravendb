using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Sharding.Handlers.Processors.Queries;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedQueriesHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/queries", "POST")]
        public async Task Post()
        {
            using (var processor = new ShardedQueriesHandlerProcessorForGet(this, HttpMethod.Post))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/queries", "GET")]
        public async Task Get()
        {
            using (var processor = new ShardedQueriesHandlerProcessorForGet(this, HttpMethod.Get))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/queries", "PATCH")]
        public async Task Patch()
        {
            using (var processor = new ShardedQueriesHandlerProcessorForPatch(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/queries/test", "PATCH")]
        public async Task PatchTest()
        {
            using (var processor = new ShardedQueriesHandlerProcessorForPatchTest(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/queries", "DELETE")]
        public async Task Delete()
        {
            using (var processor = new ShardedQueriesHandlerProcessorForDelete(this))
                await processor.ExecuteAsync();
        }
    }
}
