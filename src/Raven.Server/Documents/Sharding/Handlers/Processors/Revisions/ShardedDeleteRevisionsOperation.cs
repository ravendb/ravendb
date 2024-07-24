using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Streaming;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    internal class ShardedDeleteRevisionsManuallyOperation : IShardedOperation<DeleteRevisionsManuallyOperation.Result>
    {
        private readonly DeleteRevisionsRequest _request;

        public ShardedDeleteRevisionsManuallyOperation(DeleteRevisionsRequest request)
        {
            _request = request;
        }

        public HttpRequest HttpRequest => null;

        public DeleteRevisionsManuallyOperation.Result Combine(Dictionary<int, ShardExecutionResult<DeleteRevisionsManuallyOperation.Result>> shardsResults) =>
            new DeleteRevisionsManuallyOperation.Result { TotalDeletes = shardsResults.Values.Sum(deleted => deleted.Result.TotalDeletes) };
        
        public RavenCommand<DeleteRevisionsManuallyOperation.Result> CreateCommandForShard(int shardNumber)
        {
            return new DeleteRevisionsManuallyCommand(_request);
        }
    }
}
