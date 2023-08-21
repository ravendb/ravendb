using System.Diagnostics.CodeAnalysis;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.DataArchival
{
    internal class DataArchivalHandlerProcessorForGet : AbstractDataArchivalHandlerProcessorForGet<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public DataArchivalHandlerProcessorForGet([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override DataArchivalConfiguration GetDataArchivalConfiguration()
        {
            using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                DataArchivalConfiguration configuration;
                using (var rawRecord = RequestHandler.Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.Database.Name))
                {
                    configuration = rawRecord?.DataArchivalConfiguration;
                }
                return configuration;
            }
        }
    }
}
