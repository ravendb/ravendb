using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Smuggler
{
    internal sealed class SmugglerHandlerProcessorForExport : AbstractSmugglerHandlerProcessorForExport<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public SmugglerHandlerProcessorForExport([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.Database.Operations.GetNextOperationId();
        }

        protected override async ValueTask<IOperationResult> ExportAsync(JsonOperationContext context, IDisposable returnToContextPool, long operationId, DatabaseSmugglerOptionsServerSide options, long startDocumentEtag,
            long startRaftIndex, OperationCancelToken token)
        {
            return await RequestHandler.Database.Operations.AddLocalOperation(
                operationId,
                OperationType.DatabaseExport,
                "Export database: " + RequestHandler.DatabaseName,
                detailedDescription: null,
                onProgress => ExportDatabaseInternalAsync(options, startDocumentEtag, startRaftIndex, onProgress, context, token),
                token: token);
        }

        private async Task<IOperationResult> ExportDatabaseInternalAsync(
            DatabaseSmugglerOptionsServerSide options,
            long startDocumentEtag,
            long startRaftIndex,
            Action<IOperationProgress> onProgress,
            JsonOperationContext jsonOperationContext,
            OperationCancelToken token)
        {
            using (token)
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var source = RequestHandler.Database.Smuggler.CreateSource(startDocumentEtag, startRaftIndex, Logger);
                await using (var outputStream = GetOutputStream(RequestHandler.ResponseBodyStream(), options))
                {
                    var destination = new StreamDestination(outputStream, context, source);
                    var smuggler = RequestHandler.Database.Smuggler.Create(source, destination,
                        jsonOperationContext, options, onProgress: onProgress, token: token.Token);
                    return await smuggler.ExecuteAsync();
                }
            }
        }
    }
}
