using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Rachis
{
    internal class RachisHandlerProcessorForWaitForIndexNotifications : AbstractRachisHandlerProcessorForWaitForIndexNotifications<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RachisHandlerProcessorForWaitForIndexNotifications([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask WaitForCommandsAsync(TransactionOperationContext _, WaitForIndexNotificationRequest commands)
        {
            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                foreach (var index in commands.RaftCommandIndexes)
                {
                    await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, token.Token);
                }
            }
        }
    }
}
