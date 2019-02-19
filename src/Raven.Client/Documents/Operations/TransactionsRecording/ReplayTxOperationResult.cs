using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TransactionsRecording
{
    public class ReplayTxOperationResult : IOperationResult
    {
        public long ExecutedCommandsAmount { get; set; }

        public string Message => $"Processed {ExecutedCommandsAmount} commands.";
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(ExecutedCommandsAmount)] = ExecutedCommandsAmount,
                [nameof(Message)] = Message
            };
        }

        public bool ShouldPersist => false;
    }
}
