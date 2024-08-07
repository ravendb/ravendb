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
    internal readonly struct ShardedDeleteRevisionsOperation : IShardedOperation<DeleteRevisionsOperation.Result>
    {
        private readonly Dictionary<int, DeleteRevisionsCommand> _cmds;
        private readonly HttpContext _httpContext;

        public ShardedDeleteRevisionsOperation(HttpContext httpContext, Dictionary<int, DeleteRevisionsCommand> cmds)
        {
            _cmds = cmds;
            _httpContext = httpContext;
        }

        public HttpRequest HttpRequest => _httpContext.Request;
        public DeleteRevisionsOperation.Result Combine(Dictionary<int, ShardExecutionResult<DeleteRevisionsOperation.Result>> results) =>
            new DeleteRevisionsOperation.Result { TotalDeletes = results.Values.Sum(deleted => deleted.Result.TotalDeletes) };

        public RavenCommand<DeleteRevisionsOperation.Result> CreateCommandForShard(int shardNumber)
        {
            return _cmds[shardNumber];
        }

    }
}
