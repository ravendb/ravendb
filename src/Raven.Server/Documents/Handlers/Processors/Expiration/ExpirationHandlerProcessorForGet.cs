using System.Diagnostics.CodeAnalysis;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Expiration
{
    internal sealed class ExpirationHandlerProcessorForGet : AbstractDatabaseHandlerProcessorForGetConfiguration<DatabaseRequestHandler, DocumentsOperationContext, ExpirationConfiguration>
    {
        public ExpirationHandlerProcessorForGet([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ExpirationConfiguration GetConfiguration()
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
