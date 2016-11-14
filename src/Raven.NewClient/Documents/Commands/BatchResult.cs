using Sparrow.Json;

namespace Raven.NewClient.Client.Documents.Commands
{
    public class BatchResult
    {
        public BlittableJsonReaderArray Results { get; set; }

    }
}