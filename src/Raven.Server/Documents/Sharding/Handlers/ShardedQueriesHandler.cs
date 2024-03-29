﻿using System.Net.Http;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Queries;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedQueriesHandler : ShardedDatabaseRequestHandler
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
