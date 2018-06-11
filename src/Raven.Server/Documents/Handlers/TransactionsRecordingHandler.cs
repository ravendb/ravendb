using System;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class TransactionsRecordingHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/replay_transactions", "POST", AuthorizationStatus.ValidUser)]
        public async Task ReplayRecording()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var input = await context.ReadForMemoryAsync(RequestBodyStream(), string.Empty);
                if (false == input.TryGet("file_path", out string filePath))
                {
                    throw new Exception("Write something here");
                }

                Database.TxMerger.Replay(filePath);
            }
        }

        [RavenAction("/databases/*/start_transactions_recording", "GET", AuthorizationStatus.ValidUser)]
        public async Task StartRecording()
        {
            var filePath = GetStringQueryString("file_path");
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                //var fileName = GetStringQueryString("file_name");
                //var input = await context.ReadForMemoryAsync(RequestBodyStream(), string.Empty);
                //if (false == input.TryGet("file_path", out string filePath))
                //{
                //    throw new Exception("Write something here");
                //}

                var command = new TransactionsRecordingCommand(
                    Database.TxMerger,
                    TransactionsRecordingCommand.Instruction.Start,
                    filePath
                );

                await Database.TxMerger.Enqueue(command);
            }
        }

        [RavenAction("/databases/*/stop_transactions_recording", "POST", AuthorizationStatus.ValidUser)]
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
