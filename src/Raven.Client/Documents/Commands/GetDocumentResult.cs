using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetDocumentResult
    {
        public BlittableJsonReaderArray Includes { get; set; }

        public BlittableJsonReaderArray Results { get; set; }
    }
}