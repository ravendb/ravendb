using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Revisions
{
    public class RevertResult : IOperationResult
    {
        public RevertProgress Progress = new RevertProgress();
        public string Message { get; }
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Progress)] = Progress.ToJson(),
                [nameof(Message)] = Message,
                [nameof(ShouldPersist)] = ShouldPersist
            };
        }

        public bool ShouldPersist { get; }
    }
}
