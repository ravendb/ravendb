using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Operations;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    public readonly struct ShardedRevertRevisionsByIdOperation : IShardedOperation
    {
        private readonly Dictionary<int, Dictionary<string, string>> _shardsToDocs;

        public ShardedRevertRevisionsByIdOperation(Dictionary<int, Dictionary<string, string>> shardsToDocs)
        {
            _shardsToDocs = shardsToDocs;
        }

        public HttpRequest HttpRequest => null;

        public RavenCommand<object> CreateCommandForShard(int shardNumber)
        {
            return new RevertRevisionsByIdCommand(_shardsToDocs[shardNumber]);
        }
    }
}
