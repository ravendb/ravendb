using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class BatchResult
    {
        public BlittableJsonReaderArray Results { get; set; }

    }
}