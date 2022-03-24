using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors
{
    internal class ShardedRachisHandlerProcessorForWaitForRaftCommands : AbstractRachisHandlerProcessorForWaitForRaftCommands<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedRachisHandlerProcessorForWaitForRaftCommands([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask WaitForCommandsAsync(WaitForCommandsRequest commands)
        {
            var op = new ShardedWaitForRaftCommandsOperation(commands.RaftCommandIndexes);
            await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op, HttpContext.RequestAborted);
        }
    }
}
