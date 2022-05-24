using System;
using System.IO.Compression;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Processors.Smuggler;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Smuggler
{
    internal class ShardedSmugglerHandlerProcessorForExport : AbstractSmugglerHandlerProcessorForExport<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSmugglerHandlerProcessorForExport([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask ExportAsync(JsonOperationContext context, long? operationId)
        {
            if(operationId.HasValue == false)
                operationId = RequestHandler.DatabaseContext.Operations.GetNextOperationId();

            await RequestHandler.Export(context, RequestHandler.DatabaseContext.DatabaseName, ExportShardedDatabaseInternalAsync, RequestHandler.DatabaseContext.Operations, operationId.Value);
        }

        protected async Task<IOperationResult> ExportShardedDatabaseInternalAsync(
            DatabaseSmugglerOptionsServerSide options,
            long startDocumentEtag,
            long startRaftIndex,
            Action<IOperationProgress> onProgress,
            JsonOperationContext jsonOperationContext,
            OperationCancelToken token)
        {
            // we use here a negative number to avoid possible collision between the server and database ids
            var operationId = RequestHandler.DatabaseContext.Operations.GetNextOperationId();
            options.IsShard = true;

            await using (var outputStream = RequestHandler.GetOutputStream(RequestHandler.ResponseBodyStream(), options))
            await using (var writer = new AsyncBlittableJsonTextWriter(jsonOperationContext, new GZipStream(outputStream, CompressionMode.Compress)))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("BuildVersion");
                writer.WriteInteger(ServerVersion.Build);

                var exportOperation = new ShardedExportOperation(RequestHandler, operationId, options, writer);
                // we execute one by one so requests will not timeout since the export can take long
                await RequestHandler.ShardExecutor.ExecuteOneByOneForAllAsync(exportOperation);

                writer.WriteEndObject();
            }

            var getStateOperation = new GetShardedOperationStateOperation(HttpContext, operationId);
            return (await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(getStateOperation)).Result;
        }
    }
}
