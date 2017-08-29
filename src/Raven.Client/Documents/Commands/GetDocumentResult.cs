using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetDocumentResult
    {
        public BlittableJsonReaderObject Includes { get; set; }

        public BlittableJsonReaderArray Results { get; set; }

        public int NextPageStart { get; set; }
    }
}
