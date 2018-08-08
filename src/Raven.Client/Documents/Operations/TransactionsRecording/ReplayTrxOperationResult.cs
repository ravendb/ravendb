using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TransactionsRecording
{
    public class ReplayTrxOperationResult : IOperationResult
    {
        public long CommandsAmount { get; set; }

        public string Message => $"Processed {CommandsAmount} commands.";
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(CommandsAmount)] = CommandsAmount
            };
        }

        public bool ShouldPersist => false;
    }
}
