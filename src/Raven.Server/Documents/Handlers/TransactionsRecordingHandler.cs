using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Net.Http.Headers;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.TransactionsRecording;
using Raven.Client.Exceptions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class TransactionsRecordingHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/transactions/replay", "POST", AuthorizationStatus.ValidUser)]
        public async Task ReplayRecording()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                if (HttpContext.Request.HasFormContentType == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["Type"] = "Error",
                            ["Error"] = "Transactions replay requires form content type"
                        });
                        return;
                    }
                }
                
                var operationId = GetLongQueryString("operationId", false) ?? Database.Operations.GetNextOperationId();

                using (var operationCancelToken = CreateOperationToken())
                {
                    var result = await Database.Operations.AddOperation(
                        database: Database,
                        description: "Replay transaction commands",
                        operationType: Operations.Operations.OperationType.ReplayTransactionCommands,
                        taskFactory: progress => Task.Run(async () =>
                        {
                            var reader = new MultipartReader(MultipartRequestHelper.GetBoundary(MediaTypeHeaderValue.Parse(HttpContext.Request.ContentType),
                                MultipartRequestHelper.MultipartBoundaryLengthLimit), HttpContext.Request.Body);
                            while (true)
                            {
                                var section = await reader.ReadNextSectionAsync().ConfigureAwait(false);
                                if (section == null)
                                    break;

                                if (ContentDispositionHeaderValue.TryParse(section.ContentDisposition, out ContentDispositionHeaderValue contentDisposition) == false)
                                    continue;

                                if (MultipartRequestHelper.HasFileContentDisposition(contentDisposition))
                                {
                                    if (section.Headers.ContainsKey("Content-Encoding") && section.Headers["Content-Encoding"] == "gzip")
                                    {
                                        using (var gzipStream = new GZipStream(section.Body, CompressionMode.Decompress))
                                        {
                                            return DoReplay(progress, gzipStream, operationCancelToken.Token);
                                        }
                                    }
                                    return DoReplay(progress, section.Body, operationCancelToken.Token);
                                }
                            }
                            
                            throw new BadRequestException("Please upload source file using FormData");
                        }),
                        id: operationId,
                        token: operationCancelToken
                    );

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, result.ToJson());
                    }
                }
            }
        }

        private IOperationResult DoReplay(
            Action<IOperationProgress> onProgress,
            Stream replayStream,
            CancellationToken token)
        {
            const int commandAmountBetweenResponds = 1024;
            long commandAmountForNextRespond = commandAmountBetweenResponds;

            try
            {
                var progress = new IndeterminateProgressCount
                {
                    Processed = 0
                };
                
                // send initial progress
                onProgress(progress);
                
                long commandsProgress = 0;
                foreach (var replayProgress in ReplayTxCommandHelper.Replay(Database, replayStream))
                {
                    commandsProgress = replayProgress.CommandsProgress;
                    if (replayProgress.CommandsProgress > commandAmountForNextRespond)
                    {
                        commandAmountForNextRespond = replayProgress.CommandsProgress + commandAmountBetweenResponds;

                        progress.Processed = replayProgress.CommandsProgress;
                        onProgress(progress);
                    }

                    token.ThrowIfCancellationRequested();
                }

                return new ReplayTxOperationResult
                {
                    ExecutedCommandsAmount = commandsProgress
                };
            }
            catch (Exception e)
            {
                //Because the request is working while the file is uploading the server needs to ignore the rest of the stream
                //and the client needs to stop sending it
                HttpContext.Response.Headers["Connection"] = "close";
                throw new InvalidOperationException("Failed to process replay transaction commands", e);
            }
        }

        [RavenAction("/databases/*/admin/transactions/start-recording", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task StartRecording()
        {
            if (Database.TxMerger.RecordingEnabled)
            {
                throw new BadRequestException("Another recording is already in progress");
            }
            
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                var parameters = JsonDeserializationServer.StartTransactionsRecordingOperationParameters(json);
                if (parameters.File == null)
                {
                    ThrowRequiredPropertyNameInRequest(nameof(parameters.File));
                }

                var tcs = new TaskCompletionSource<IOperationResult>(TaskCreationOptions.RunContinuationsAsynchronously); 
                var operationId = ServerStore.Operations.GetNextOperationId();
                
                var command = new StartTransactionsRecordingCommand(
                        Database.TxMerger,
                        parameters.File,
                        () => tcs.SetResult(null) // we don't provide any completion details
                    );

                await Database.TxMerger.Enqueue(command);

                var task = ServerStore.Operations.AddOperation(null,
                    "Recording for: '" + Database.Name + ". Output file: '" + parameters.File + "'",
                    Operations.Operations.OperationType.RecordTransactionCommands,
                    progress =>
                    {
                        // push this notification to studio
                        progress(null);
                        
                        return tcs.Task;
                    },
                    operationId, 
                    new RecordingDetails
                    {
                        DatabaseName = Database.Name,
                        FilePath = parameters.File
                    });
                
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationId(context, operationId);
                }
            }
        }

        [RavenAction("/databases/*/admin/transactions/stop-recording", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task StopRecording()
        {
            var command = new StopTransactionsRecordingCommand(Database.TxMerger);

            await Database.TxMerger.Enqueue(command);
            NoContentStatus();
        }
        
        public class RecordingDetails : IOperationDetailedDescription
        {
            public string DatabaseName { get; set; }
            
            public string FilePath { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(DatabaseName)] = DatabaseName,
                    [nameof(FilePath)] = FilePath
                };
            }
        }
    }

    public class StartTransactionsRecordingCommand : TransactionOperationsMerger.MergedTransactionCommand
    {
        private readonly TransactionOperationsMerger _databaseTxMerger;
        private readonly string _filePath;
        private readonly Action _onStop;

        public StartTransactionsRecordingCommand(TransactionOperationsMerger databaseTxMerger, string filePath, Action onStop)
        {
            _databaseTxMerger = databaseTxMerger;
            _filePath = filePath;
            _onStop = onStop;
        }

        public override int Execute(DocumentsOperationContext context, TransactionOperationsMerger.RecordingState recordingState)
        {
            return ExecuteDirectly(context);
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            return null;
        }

        protected override int ExecuteCmd(DocumentsOperationContext context)
        {
            _databaseTxMerger.StartRecording(_filePath, _onStop);
            return 0;
        }
    }
    
    public class StopTransactionsRecordingCommand : TransactionOperationsMerger.MergedTransactionCommand
    {
        private readonly TransactionOperationsMerger _databaseTxMerger;

        public StopTransactionsRecordingCommand(TransactionOperationsMerger databaseTxMerger)
        {
            _databaseTxMerger = databaseTxMerger;
        }

        public override int Execute(DocumentsOperationContext context, TransactionOperationsMerger.RecordingState recordingState)
        {
            return ExecuteDirectly(context);
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            return null;
        }

        protected override int ExecuteCmd(DocumentsOperationContext context)
        {
            _databaseTxMerger.StopRecording();
            return 0;
        }
    }
}
