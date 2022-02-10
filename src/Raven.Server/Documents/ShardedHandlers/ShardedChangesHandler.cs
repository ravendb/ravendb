// ------------------------------------------------------------[-----------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedChangesHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/changes", "GET")]
        public async Task GetChanges()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                //TODO:
                await Task.Delay(TimeSpan.FromDays(10));
            }
        }
       
    }
}
