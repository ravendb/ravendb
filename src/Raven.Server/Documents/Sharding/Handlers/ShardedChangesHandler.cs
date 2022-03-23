// ------------------------------------------------------------[-----------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedChangesHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/changes", "GET")]
        public async Task GetChanges()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Marcin, DevelopmentHelper.Severity.Normal, "handle this");

            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                //TODO:
                await Task.Delay(TimeSpan.FromDays(10));
            }
        }
       
    }
}
