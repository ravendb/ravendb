using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class BatchResult
    {
        public BlittableJsonReaderArray Commands { get; set; }

    }
}