using System.Diagnostics.CodeAnalysis;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Expiration
{
    internal class ExpirationHandlerProcessorForGet : AbstractExpirationHandlerProcessorForGet<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ExpirationHandlerProcessorForGet([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ExpirationConfiguration GetExpirationConfiguration()
        {
            using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                ExpirationConfiguration configuration;
                using (var rawRecord = RequestHandler.Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.Database.Name))
                {
                    configuration = rawRecord?.ExpirationConfiguration;
                }
                return configuration;
            }
        }
    }
}
