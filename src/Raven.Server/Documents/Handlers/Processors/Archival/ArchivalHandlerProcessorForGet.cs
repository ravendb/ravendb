using System.Diagnostics.CodeAnalysis;
using Raven.Client.Documents.Operations.Archival;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Archival
{
    internal class ArchivalHandlerProcessorForGet : AbstractArchivalHandlerProcessorForGet<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ArchivalHandlerProcessorForGet([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ArchivalConfiguration GetArchivalConfiguration()
        {
            using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                ArchivalConfiguration configuration;
                using (var rawRecord = RequestHandler.Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.Database.Name))
                {
                    configuration = rawRecord?.ArchivalConfiguration;
                }
                return configuration;
            }
        }
    }
}
