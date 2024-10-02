using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Net.Http.Headers;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.TransactionsRecording;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.TransactionMerger;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public sealed class TransactionsRecordingHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/transactions/replay", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task ReplayRecording()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                if (HttpContext.Request.HasFormContentType == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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

                using (var token = CreateHttpRequestBoundOperationToken())
                {
                    var result = await Database.Operations.AddLocalOperation(
                        operationId,
                        operationType: OperationType.ReplayTransactionCommands,
                        description: "Replay transaction commands",
                        detailedDescription: null,
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
                                    await using (var stream = GetDecompressedStream(section.Body, section.Headers))
                                        return await DoReplayAsync(progress, stream, token.Token);
                                }
                            }

                            throw new BadRequestException("Please upload source file using FormData");
                        }),
                        token: token
                    );

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, result.ToJson());
                    }
                }
            }
        }

        private async Task<IOperationResult> DoReplayAsync(
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
                await foreach (var replayProgress in ReplayTxCommandHelper.ReplayAsync(Database, replayStream))
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

        [RavenAction("/databases/*/admin/transactions/start-recording", "POST", AuthorizationStatus.DatabaseAdmin)]
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
                var outputFilePath = parameters.File;

                if (outputFilePath == null)
                {
                    ThrowRequiredPropertyNameInRequest(nameof(parameters.File));
                }

                if (File.Exists(outputFilePath))
                {
                    throw new InvalidOperationException("File " + outputFilePath + " already exists");
                }

                // here path is either a new file -or- an existing directory
                if (Directory.Exists(outputFilePath))
                {
                    throw new InvalidOperationException(outputFilePath + " is a directory. Please enter a path to a file.");
                }

                var tcs = new TaskCompletionSource<IOperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                var operationId = ServerStore.Operations.GetNextOperationId();

                var command = new StartTransactionsRecordingCommand<DocumentsOperationContext, DocumentsTransaction>(
                        Database.TxMerger,
                        parameters.File,
                        () => tcs.SetResult(null) // we don't provide any completion details
                    );

                await Database.TxMerger.Enqueue(command);

                var task = ServerStore.Operations.AddLocalOperation(
                    operationId,
                    OperationType.RecordTransactionCommands,
                    "Recording for: '" + Database.Name + ". Output file: '" + parameters.File + "'",
                    detailedDescription: new RecordingDetails
                    {
                        DatabaseName = Database.Name,
                        FilePath = parameters.File
                    },
                    progress =>
                    {
                        // push this notification to studio
                        progress(null);

                        return tcs.Task;
                    });

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                }
            }
        }

        [RavenAction("/databases/*/admin/transactions/stop-recording", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task StopRecording()
        {
            var command = new StopTransactionsRecordingCommand<DocumentsOperationContext, DocumentsTransaction>(Database.TxMerger);

            await Database.TxMerger.Enqueue(command);
            NoContentStatus();
        }

        public sealed class RecordingDetails : IOperationDetailedDescription
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

    public sealed class StartTransactionsRecordingCommand<TOperationContext, TTransaction> : MergedTransactionCommand<TOperationContext, TTransaction>
        where TOperationContext : TransactionOperationContext<TTransaction>
        where TTransaction : RavenTransaction
    {
        private readonly AbstractTransactionOperationsMerger<TOperationContext, TTransaction> _txMerger;
        private readonly string _filePath;
        private readonly Action _onStop;

        public StartTransactionsRecordingCommand(AbstractTransactionOperationsMerger<TOperationContext, TTransaction> txMerger, string filePath, Action onStop)
        {
            _txMerger = txMerger;
            _filePath = filePath;
            _onStop = onStop;
        }

        public override long Execute(TOperationContext context, AbstractTransactionOperationsMerger<TOperationContext, TTransaction>.RecordingState recordingState)
        {
            return ExecuteDirectly(context);
        }

        public override IReplayableCommandDto<TOperationContext, TTransaction, MergedTransactionCommand<TOperationContext, TTransaction>> ToDto(TOperationContext context)
        {
            return null;
        }

        protected override long ExecuteCmd(TOperationContext context)
        {
            _txMerger.StartRecording(_filePath, _onStop);
            return 0;
        }
    }

    public sealed class StopTransactionsRecordingCommand<TOperationContext, TTransaction> : MergedTransactionCommand<TOperationContext, TTransaction>
        where TOperationContext : TransactionOperationContext<TTransaction>
        where TTransaction : RavenTransaction
    {
        private readonly AbstractTransactionOperationsMerger<TOperationContext, TTransaction> _txMerger;

        public StopTransactionsRecordingCommand(AbstractTransactionOperationsMerger<TOperationContext, TTransaction> _txMerger)
        {
            this._txMerger = _txMerger;
        }

        public override long Execute(TOperationContext context, AbstractTransactionOperationsMerger<TOperationContext, TTransaction>.RecordingState recordingState)
        {
            return ExecuteDirectly(context);
        }

        public override IReplayableCommandDto<TOperationContext, TTransaction, MergedTransactionCommand<TOperationContext, TTransaction>> ToDto(TOperationContext context)
        {
            return null;
        }

        protected override long ExecuteCmd(TOperationContext context)
        {
            _txMerger.StopRecording();
            return 0;
        }
    }
}
