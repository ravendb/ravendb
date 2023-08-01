using Raven.Client.Documents.Commands;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations
{
    internal sealed class GetCollectionFieldsOperation
    {
        private BlittableJsonReaderObject _result;
        private readonly GetCollectionFieldsCommand _command;

        internal GetCollectionFieldsCommand Command => _command;

        public GetCollectionFieldsOperation(string collection, string prefix)
        {
            _command = new GetCollectionFieldsCommand(collection, prefix);
        }

        public GetCollectionFieldsCommand CreateRequest() => _command;

        public void SetResult(BlittableJsonReaderObject result)
        {
            _result = result;
        }

        public BlittableJsonReaderObject GetCollectionFields() => _result;
    }
}
