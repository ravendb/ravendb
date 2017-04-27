using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Operations;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers
{
    public class BulkInsertHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulk_insert", "POST")]
        public async Task BulkInsert()
        {
            var operationCancelToken = CreateOperationToken();
            var id = GetLongQueryString("id");

            await Database.Operations.AddOperation("Bulk Insert", DatabaseOperations.OperationType.BulkInsert,
                progress => DoBulkInsert(progress, operationCancelToken.Token),
                id.Value,
                operationCancelToken
            );
        }

        private async Task<IOperationResult> DoBulkInsert(Action<IOperationProgress> onProgress, CancellationToken token)
        {
            var progress = new BulkInsertProgress();
            try
            {
                var logger = LoggingSource.Instance.GetLogger<MergedInsertBulkCommand>(Database.Name);
                IDisposable currentCtxReset = null, previousCtxReset = null;
                try
                {
                    JsonOperationContext context;
                    using (ContextPool.AllocateOperationContext(out context))
                    using (var buffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance())
                    {
                        JsonOperationContext docsCtx;
                        currentCtxReset = ContextPool.AllocateOperationContext(out docsCtx);
                        var requestBodyStream = RequestBodyStream();

                        using (var parser = new BatchRequestParser.ReadMany(context, requestBodyStream, buffer,token))
                        {
                            await parser.Init();

                            var list = new List<BatchRequestParser.CommandData>();
                            long totalSize = 0;
                            while (true)
                            {                               
                                var task = parser.MoveNext(docsCtx);
                                if (task == null)
                                    break;

                                token.ThrowIfCancellationRequested();

                                    // if we are going to wait on the network, flush immediately
                                if ((task.IsCompleted == false && list.Count> 0) || 
                                    // but don't batch too much anyway
                                    totalSize > 16 * Voron.Global.Constants.Size.Megabyte)
                                {
                                    await Database.TxMerger.Enqueue(new MergedInsertBulkCommand
                                    {
                                        Commands = list,
                                        Database = Database,
                                        Logger = logger,
                                        TotalSize = totalSize
                                    });

                                    progress.BatchCount++;
                                    progress.Processed += list.Count;
                                    progress.LastProcessedId = list.Last().Key;

                                    onProgress(progress);

                                    previousCtxReset?.Dispose();
                                    previousCtxReset = currentCtxReset;
                                    currentCtxReset = ContextPool.AllocateOperationContext(out docsCtx);
                                    
                                    list.Clear();
                                    totalSize = 0;
                                }

                                var commandData = await task;
                                if (commandData.Method == BatchRequestParser.CommandType.None)
                                    break;

                                totalSize += commandData.Document.Size;
                                list.Add(commandData);
                                
                            }
                            if (list.Count > 0)
                            {
                                await Database.TxMerger.Enqueue(new MergedInsertBulkCommand
                                {
                                    Commands = list,
                                    Database = Database,
                                    Logger = logger,
                                    TotalSize = totalSize
                                });

                                progress.BatchCount++;
                                progress.Processed += list.Count;
                                progress.LastProcessedId = list.Last().Key;

                                onProgress(progress);
                            }
                        }
                    }
                }
                finally
                {
                    currentCtxReset?.Dispose();
                    previousCtxReset?.Dispose();
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                return new BulkOperationResult
                {
                    Total = progress.Processed
                };
            }
            catch (Exception e)
            {
                HttpContext.Response.Headers["Connection"] = "close";
                throw new InvalidOperationException("Failed to process bulk insert " + progress.ToString(), e);
            }
        }


        private class MergedInsertBulkCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public Logger Logger;
            public DocumentDatabase Database;
            public List<BatchRequestParser.CommandData> Commands;
            public long TotalSize;
            public override int Execute(DocumentsOperationContext context)
            {
                foreach (var cmd in Commands)
                {
                    Debug.Assert(cmd.Method == BatchRequestParser.CommandType.PUT);
                    Database.DocumentsStorage.Put(context, cmd.Key, null, cmd.Document);
                }
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"Merged {Commands.Count:#,#;;0} operations and {Math.Round(TotalSize / 1024d, 1):#,#.#;;0} kb");
                }
                return Commands.Count;
            }
        }
    }
}
