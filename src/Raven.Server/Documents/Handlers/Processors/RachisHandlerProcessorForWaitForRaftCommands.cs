using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal class RachisHandlerProcessorForWaitForRaftCommands : AbstractRachisHandlerProcessorForWaitForRaftCommands<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RachisHandlerProcessorForWaitForRaftCommands([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask WaitForCommandsAsync(TransactionOperationContext _, WaitForCommandsRequest commands)
        {
            foreach (var index in commands.RaftCommandIndexes)
            {
                await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, HttpContext.RequestAborted);
            }
        }
    }
}
