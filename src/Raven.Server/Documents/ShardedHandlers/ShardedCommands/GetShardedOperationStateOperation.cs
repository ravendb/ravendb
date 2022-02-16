using System;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public readonly struct GetShardedOperationStateOperation : IShardedOperation<OperationState>
    {
        private readonly long _id;
        private readonly string _nodeTag;

        public GetShardedOperationStateOperation(long id, string nodeTag = null)
        {
            _id = id;
            _nodeTag = nodeTag;
        }
        public RavenCommand<OperationState> CreateCommandForShard(int shard) => new GetOperationStateOperation.GetOperationStateCommand(_id, _nodeTag);

        public OperationState Combine(Memory<OperationState> results)
        {
            var combined = new OperationState
            {
                Result = new SmugglerResult()
            };

            for (int i = 0; i < results.Length; i++)
            {
                GetOperationStateOperation.GetOperationStateCommand.CombineSmugglerResults(combined.Result, results.Span[i].Result);
            }

            return combined;
        }
    }
}
