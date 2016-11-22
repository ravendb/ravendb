using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class GetDocumentResult
    {
        public BlittableJsonReaderArray Includes { get; set; }

        public BlittableJsonReaderArray Results { get; set; }
    }
}