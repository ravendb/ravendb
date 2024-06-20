using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Operations;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    public readonly struct ShardedRevertDocumentsToRevisionsOperation : IShardedOperation
    {
        private readonly Dictionary<long, Dictionary<string, string>> _shardsToDocs;

        public ShardedRevertDocumentsToRevisionsOperation(Dictionary<long, Dictionary<string, string>> shardsToDocs)
        {
            _shardsToDocs = shardsToDocs;
        }

        public HttpRequest HttpRequest => null;

        public RavenCommand<object> CreateCommandForShard(int shardNumber)
        {
            if (_shardsToDocs.Keys.Contains(shardNumber) == false)
                return null;

            return new RevertDocumentsToRevisionsCommand(_shardsToDocs[shardNumber]);
        }
    }
}
