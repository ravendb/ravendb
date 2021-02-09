using System;
using System.Collections.Generic;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public class FetchDocumentsFromShardsCommand : ShardedCommand
    {
        public List<int> PositionMatches;

        public FetchDocumentsFromShardsCommand(ShardedRequestHandler handler) : base(handler, ShardedCommands.Headers.None)
        {
        }
    }
}
