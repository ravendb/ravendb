using System;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Processors.Smuggler;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Smuggler
{
    internal class ShardedSmugglerHandlerProcessorForImport : AbstractSmugglerHandlerProcessorForImport<ShardedSmugglerHandler, TransactionOperationContext>
    {
        public ShardedSmugglerHandlerProcessorForImport([NotNull] ShardedSmugglerHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask ImportAsync(JsonOperationContext context, long? operationId)
        {
            operationId ??= RequestHandler.DatabaseContext.Operations.GetNextOperationId();
            await Import(context, RequestHandler.DatabaseContext.DatabaseName, DoImportInternalAsync, RequestHandler.DatabaseContext.Operations, operationId.Value);
        }

        private async Task DoImportInternalAsync(
            JsonOperationContext jsonOperationContext,
            Stream stream,
            DatabaseSmugglerOptionsServerSide options,
            SmugglerResult result,
            Action<IOperationProgress> onProgress,
            long operationId,
            OperationCancelToken token)
        {
            using (var source = new StreamSource(stream, jsonOperationContext, RequestHandler.DatabaseContext.DatabaseName, options))
            {
                DatabaseRecord record;
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    record = ServerStore.Cluster.ReadDatabase(ctx, RequestHandler.DatabaseContext.DatabaseName);
                }

                var smuggler = new ShardedDatabaseSmuggler(source, jsonOperationContext, record, ServerStore, RequestHandler.DatabaseContext, RequestHandler, options, result, operationId, onProgress, token: token.Token);

                await smuggler.ExecuteAsync();
            }
        }
    }
}
