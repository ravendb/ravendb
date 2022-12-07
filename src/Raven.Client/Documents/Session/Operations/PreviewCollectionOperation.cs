using Raven.Client.Documents.Commands;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations
{
    public class PreviewCollectionOperation
    {
        private BlittableJsonReaderObject _result;
        private readonly PreviewCollectionCommand _command;

        internal PreviewCollectionCommand Command => _command;

        public PreviewCollectionOperation(string collection)
        {
            _command = new PreviewCollectionCommand(collection);
        }

        public PreviewCollectionCommand CreateRequest() => _command;

        public void SetResult(BlittableJsonReaderObject result)
        {
            _result = result;
        }

        public BlittableJsonReaderObject PreviewCollection() => _result;
    }
}
