using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents.Handlers.Processors.Smuggler;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Sparrow.Json;
using BackupUtils = Raven.Server.Utils.BackupUtils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Smuggler
{
    internal sealed class ShardedSmugglerHandlerProcessorForExport : AbstractSmugglerHandlerProcessorForExport<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSmugglerHandlerProcessorForExport([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
        }

        protected override async ValueTask<IOperationResult> ExportAsync(JsonOperationContext context, IDisposable returnToContextPool, long operationId,
            DatabaseSmugglerOptionsServerSide options, long startDocumentEtag,
            long startRaftIndex, OperationCancelToken token)
        {
            using (returnToContextPool)
            {
                return await RequestHandler.DatabaseContext.Operations.AddLocalOperation(
                    operationId,
                    OperationType.DatabaseExport,
                    "Export database: " + RequestHandler.DatabaseName,
                    detailedDescription: null,
                    onProgress => ExportShardedDatabaseInternalAsync(options, onProgress, context, token),
                    token: token);
            }
        }

        private async Task<IOperationResult> ExportShardedDatabaseInternalAsync(
            DatabaseSmugglerOptionsServerSide options,
            Action<IOperationProgress> onProgress,
            JsonOperationContext jsonOperationContext,
            OperationCancelToken token)
        {
            options.IsShard = true;

            var shardedOperationStateResult = new ShardedSmugglerResult()
            {
                Results = new List<ShardNodeSmugglerResult>()
            };

            await using (var outputStream = await GetOutputStreamAsync(RequestHandler.ResponseBodyStream(), options))
            await using (var writer = new AsyncBlittableJsonTextWriter(jsonOperationContext, BackupUtils.GetCompressionStream(outputStream, options.CompressionAlgorithm ?? RequestHandler.DatabaseContext.Configuration.ExportImport.CompressionAlgorithm, RequestHandler.DatabaseContext.Configuration.ExportImport.CompressionLevel)))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("BuildVersion");
                writer.WriteInteger(ServerVersion.Build);
                
                // we execute one by one so requests will not timeout since the export can take long
                foreach (var shardNumber in RequestHandler.DatabaseContext.ShardsTopology.Keys)
                {
                    var smuggler = new DatabaseSmuggler(
                        (_, nodeTag) => RequestHandler.DatabaseContext.Operations.GetChanges(new ShardedDatabaseIdentifier(nodeTag, shardNumber)),
                        _ => RequestHandler.DatabaseContext.ShardExecutor.GetRequestExecutorAt(shardNumber),
                        ShardHelper.ToShardName(RequestHandler.DatabaseContext.DatabaseName, shardNumber));

                    var smugglerOperation = await smuggler.ExportToStreamAsync(options.ToExportOptions(), async stream =>
                    {
                        await using (var gzipStream = await BackupUtils.GetDecompressionStreamAsync(await GetInputStreamAsync(stream, options)))
                        {
                            await writer.WriteStreamAsync(gzipStream);
                        }
                    } , token.Token);

                    smugglerOperation.OnProgressChanged += (sender, progress) =>
                    {
                        if (progress is not SmugglerResult.SmugglerProgress sp)
                            return;

                        var shardProgress = new ShardedSmugglerProgress();
                        shardProgress.Fill(sp, shardNumber, smugglerOperation.NodeTag);
                        onProgress(shardProgress);
                    };

                    var smugglerResult = await smugglerOperation.WaitForCompletionAsync<SmugglerResult>();

                    shardedOperationStateResult.Results.Add(new ShardNodeSmugglerResult()
                    {
                        NodeTag = smugglerOperation.NodeTag,
                        ShardNumber = shardNumber,
                        Result = smugglerResult
                    });
                }

                writer.WriteEndObject();
            }

            return shardedOperationStateResult;
        }
    }
}
