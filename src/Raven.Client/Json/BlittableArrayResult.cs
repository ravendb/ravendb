using Sparrow.Json;

namespace Raven.Client.Json
{
    public sealed class BlittableArrayResult
    {
        public BlittableJsonReaderArray Results { get; set; }
        public long TotalResults { get; set; }
        public string ContinuationToken { get; set; }
    }

    public sealed class BatchCommandResult
    {
        public BlittableJsonReaderArray Results { get; set; }
        public long? TransactionIndex { get; set; }
    }
}
