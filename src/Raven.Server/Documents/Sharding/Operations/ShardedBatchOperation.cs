using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers.Batches;
using Raven.Server.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Operations
{
    public readonly struct ShardedBatchOperation : IShardedOperation<BlittableJsonReaderObject, DynamicJsonArray>
    {
        private readonly HttpContext _httpContext;
        private readonly JsonOperationContext _resultContext;
        private readonly Dictionary<int, ShardedSingleNodeBatchCommand> _commandsPerShard;
        private readonly ShardedBatchCommand _command;

        public ShardedBatchOperation(HttpContext httpContext,
            JsonOperationContext resultContext,
            Dictionary<int, ShardedSingleNodeBatchCommand> commandsPerShard,
            ShardedBatchCommand command)
        {
            _httpContext = httpContext;
            _resultContext = resultContext;
            _commandsPerShard = commandsPerShard;
            _command = command;
        }

        public HttpRequest HttpRequest => _httpContext.Request;

        public DynamicJsonArray Combine(Dictionary<int, ShardExecutionResult<BlittableJsonReaderObject>> results)
        {
            ShardMismatchException lastMismatchException = null;
            foreach (var c in _commandsPerShard.Values)
            {
                var executionResult = results[c.ShardNumber];

                try
                {
                    if (executionResult.CommandTask.IsCompletedSuccessfully == false)
                        executionResult.CommandTask.GetAwaiter().GetResult(); // should throw immediately
                }
                catch (ShardMismatchException e)
                {
                    // will retry only for this type of exception
                    lastMismatchException = e;
                    continue;
                }

                _command.MarkShardAsComplete(_resultContext, c.ShardNumber, HttpRequest.IsFromStudio());
            }

            if (lastMismatchException != null)
                throw lastMismatchException;

            return _command.Result;
        }

        public RavenCommand<BlittableJsonReaderObject> CreateCommandForShard(int shardNumber) => _commandsPerShard[shardNumber];
    }
}
