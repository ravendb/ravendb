// -----------------------------------------------------------------------
//  <copyright file="ShardedHiLoHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers.Processors.HiLo;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedHiLoHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/hilo/next", "GET")]
        public async Task GetNextHiLo()
        {
            using (var processor = new ShardedHiLoHandlerProcessorForGetNextHiLo(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/hilo/return", "PUT")]
        public async Task ReturnHiLo()
        {
            using (var processor = new ShardedHiLoHandlerProcessorForReturnHiLo(this))
                await processor.ExecuteAsync();
        }
    }
}
