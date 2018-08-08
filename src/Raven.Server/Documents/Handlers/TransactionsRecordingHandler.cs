using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.TransactionsRecording;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class TransactionsRecordingHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/transactions/replay", "POST", AuthorizationStatus.ValidUser)]
        public Task ReplayRecording()
        {
            var operationCancelToken = CreateOperationToken();
            var operationId = Database.Operations.GetNextOperationId();

            Stream stream = RequestBodyStream();
            //Todo to use zip
            //using (var gZipStreamDocuments = new GZipStream(fileStream, CompressionMode.Compress, true))

            //Todo To think how to deal with HttpRequestStream dispose without using new and is it enough to register dispaosable to token
            var replayStream = new MemoryStream();
            operationCancelToken.Token.Register(replayStream.Dispose);
            stream.CopyTo(replayStream);
            replayStream.Seek(0, SeekOrigin.Begin);

            var task = Database.Operations.AddOperation(
                database: Database,
                description: "Replay transaction commands",
                operationType: Operations.Operations.OperationType.ReplayTransactionCommands,
                taskFactory: async progress => await DoReplay(progress, replayStream, operationCancelToken.Token),
                id: operationId,
                token: operationCancelToken
            );
            
            using(ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationId(context, operationId);
            }

            task.ContinueWith(_ =>
            {
                operationCancelToken.Dispose();
            });

            return Task.CompletedTask;
        }

        private async Task<IOperationResult> DoReplay(
            Action<IOperationProgress> onProgress,
            Stream replayStream,
            CancellationToken token)
        {
            const int commandAmountBetweenRespond = 15;

            try
            {
                return await Task.Run(() =>
                {

                    long commandsProgress = 0;
                    var streamLength = replayStream.Length;
                    foreach (var replayProgress in Database.TxMerger.Replay(replayStream))
                    {
                        commandsProgress = replayProgress.CommandsProgress;
                        //Todo Maybe should be relative to time of size
                        if (replayProgress.CommandsProgress % commandAmountBetweenRespond == 0)
                        {
                            onProgress(new ReplayTrxProgress
                            {
                                ProcessedCommand = replayProgress.CommandsProgress,
                                ProcessedPercentage = (int)(100 * replayProgress.StreamProgress / streamLength)
                            });
                        }

                        token.ThrowIfCancellationRequested();
                    }

                    return new ReplayTrxOperationResult
                    {
                        CommandsAmount = commandsProgress
                    };
                }, token);
            }
            catch (Exception e)
            {
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
