using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Smuggler
{
    internal class SmugglerHandlerProcessorForExport : AbstractSmugglerHandlerProcessorForExport<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public SmugglerHandlerProcessorForExport([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask ExportAsync(JsonOperationContext context, long? operationId)
        {
            if(operationId.HasValue == false)
                operationId = RequestHandler.Database.Operations.GetNextOperationId();

            await RequestHandler.Export(context, RequestHandler.DatabaseName, ExportDatabaseInternalAsync, RequestHandler.Database.Operations, operationId.Value);
        }

        protected async Task<IOperationResult> ExportDatabaseInternalAsync(
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
                var source = new DatabaseSource(RequestHandler.Database, startDocumentEtag, startRaftIndex, Logger);
                await using (var outputStream = RequestHandler.GetOutputStream(RequestHandler.ResponseBodyStream(), options))
                {
                    var destination = new StreamDestination(outputStream, context, source);
                    var smuggler = SmugglerBase.GetDatabaseSmuggler(RequestHandler.Database, source, destination, RequestHandler.Database.Time,
                        jsonOperationContext, options, onProgress: onProgress, token: token.Token);
                    return await smuggler.ExecuteAsync();
                }
            }
        }
    }
}
