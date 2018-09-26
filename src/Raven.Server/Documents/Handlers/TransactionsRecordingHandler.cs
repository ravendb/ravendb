using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.TransactionsRecording;
using Raven.Client.Documents.Session;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class TransactionsRecordingHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/transactions/replay", "POST", AuthorizationStatus.ValidUser)]
        public async Task ReplayRecording()
        {
            var operationId = GetLongQueryString("operationId");
            using (var operationCancelToken = CreateOperationToken())
            {
                var replayStream = RequestBodyStream();

                var result = await Database.Operations.AddOperation(
                    database: Database,
                    description: "Replay transaction commands",
                    operationType: Operations.Operations.OperationType.ReplayTransactionCommands,
                    taskFactory: progress => Task.Run(() => DoReplay(progress, replayStream, operationCancelToken.Token)),
                    id: operationId,
                    token: operationCancelToken
                );

                using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result.ToJson());
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
                long commandsProgress = 0;
                var stopwatch = Stopwatch.StartNew();
                stopwatch.Start();
                foreach (var replayProgress in ReplayTxCommandHelper.Replay(Database, replayStream))
                {
                    commandsProgress = replayProgress.CommandsProgress;
                    if (replayProgress.CommandsProgress > commandAmountForNextRespond)
                    {
                        commandAmountForNextRespond = replayProgress.CommandsProgress + commandAmountBetweenResponds;
                        onProgress(new ReplayTxProgress
                        {
                            ProcessedCommand = replayProgress.CommandsProgress,
                            PassedTime = stopwatch.Elapsed
                        });
                    }

                    token.ThrowIfCancellationRequested();
                }
                stopwatch.Stop();

                return new ReplayTxOperationResult
                {
                    ExecutedCommandsAmount = commandsProgress,
                    PassedTime = stopwatch.Elapsed
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
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                var parameters = JsonDeserializationServer.StartTransactionsRecordingOperationParameters(json);
                if (parameters.File == null)
                {
                    ThrowRequiredPropertyNameInRequest(nameof(parameters.File));
                }

                var command = new TransactionsRecordingCommand(
                        Database.TxMerger,
                        TransactionsRecordingCommand.Instruction.Start,
                        parameters.File
                    );

                await Database.TxMerger.Enqueue(command);

                NoContentStatus();
            }
        }

        [RavenAction("/databases/*/admin/transactions/stop-recording", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task StopRecording()
        {
            var command = new TransactionsRecordingCommand(
                Database.TxMerger,
                TransactionsRecordingCommand.Instruction.Stop
            );

            await Database.TxMerger.Enqueue(command);
            NoContentStatus();
        }
    }

    public class TransactionsRecordingCommand : TransactionOperationsMerger.MergedTransactionCommand
    {
        public enum Instruction
        {
            Stop,
            Start
        }

        private readonly TransactionOperationsMerger _databaseTxMerger;
        private readonly Instruction _instruction;
        private readonly string _filePath;

        public TransactionsRecordingCommand(TransactionOperationsMerger databaseTxMerger, Instruction instruction, string filePath = null)
        {
            _databaseTxMerger = databaseTxMerger;
            _instruction = instruction;
            _filePath = filePath;
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
            switch (_instruction)
            {
                case Instruction.Start:
                    _databaseTxMerger.StartRecording(_filePath);
                    break;
                case Instruction.Stop:
                    _databaseTxMerger.StopRecording();
                    break;
                default:
                    throw new ArgumentOutOfRangeException($"The value {_instruction} was out of the range of valid values");
            }
            return 0;
        }

        public override string ToString()
        {
            return base.ToString() + $", {nameof(Instruction)}:{_instruction}";
        }
    }
}
