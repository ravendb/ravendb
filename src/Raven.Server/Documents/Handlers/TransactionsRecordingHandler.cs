using System;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class TransactionsRecordingHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/transactions/replay", "POST", AuthorizationStatus.ValidUser)]
        public Task ReplayRecording()
        {
            var replayStream = RequestBodyStream();
            //Todo to use zip
            //using (var gZipStreamDocuments = new GZipStream(fileStream, CompressionMode.Compress, true))
            Database.TxMerger.Replay(replayStream);

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/transactions/start-recording", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task StartRecording()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                const string filePropertyName = "file";
                var input = await context.ReadForMemoryAsync(RequestBodyStream(), "body request");
                if (false == input.TryGet(filePropertyName, out string filePath))
                {
                    ThrowRequiredPropertyNameInRequest(filePropertyName);
                }

                var command = new TransactionsRecordingCommand(
                        Database.TxMerger,
                        TransactionsRecordingCommand.Instruction.Start,
                        filePath
                    );

                await Database.TxMerger.Enqueue(command);
            }
        }

        [RavenAction("/databases/*/transactions/stop-recording", "POST", AuthorizationStatus.ClusterAdmin)]
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
                    throw new ArgumentOutOfRangeException();
            }
            return 0;
        }

        public override string ToString()
        {
            return base.ToString() + $", {nameof(Instruction)}:{_instruction}";
        }
    }
}
