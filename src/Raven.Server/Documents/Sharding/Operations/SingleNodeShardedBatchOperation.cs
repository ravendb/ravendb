using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers.Batches;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations
{
    public readonly struct SingleNodeShardedBatchOperation : IShardedOperation<BlittableJsonReaderObject, Exception>
    {
        private readonly HttpContext _httpContext;
        private readonly JsonOperationContext _resultContext;
        private readonly Dictionary<int, ShardedSingleNodeBatchCommand> _commands;
        private readonly ShardedBatchCommand _command;
        private readonly object[] _result;

        public SingleNodeShardedBatchOperation(
            HttpContext httpContext, 
            JsonOperationContext resultContext,
            Dictionary<int, ShardedSingleNodeBatchCommand> commands, 
            ShardedBatchCommand command,
            object[] result)
        {
            _httpContext = httpContext;
            _resultContext = resultContext;
            _commands = commands;
            _command = command;
            _result = result;
        }

        public HttpRequest HttpRequest => _httpContext.Request;

        public Exception Combine(Dictionary<int, ShardExecutionResult<BlittableJsonReaderObject>> results)
        {
            ShardMismatchException lastMismatchException = null;
            foreach (var c in _commands.Values)
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

                c.AssembleShardedReply(_resultContext, _result);
                _command.Skip ??= new HashSet<int>();

                foreach (var p in c.PositionInResponse)
                {
                    _command.Skip.Add(p);
                }
            }

            return lastMismatchException;
        }

        public RavenCommand<BlittableJsonReaderObject> CreateCommandForShard(int shardNumber) => _commands[shardNumber];
    }
}
