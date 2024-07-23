using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Operations;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    internal class ShardedDeleteRevisionsOperation : IShardedOperation
    {
        private readonly Dictionary<int, DeleteRevisionsRequest> _requestPerShard;

        public ShardedDeleteRevisionsOperation(Dictionary<int, DeleteRevisionsRequest> requestPerShard)
        {
            _requestPerShard = requestPerShard;
        }

        public HttpRequest HttpRequest => null;
        public RavenCommand<object> CreateCommandForShard(int shardNumber)
        {
            return new DeleteRevisionsCommand(_requestPerShard[shardNumber]);
        }
    }
}
