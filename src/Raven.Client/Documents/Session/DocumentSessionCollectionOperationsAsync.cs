using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public class DocumentSessionCollectionOperations : AdvancedSessionExtensionBase, ICollectionSessionOperations
    {
        public DocumentSessionCollectionOperations(DocumentSession session) : base(session)
        {

        }

        public BlittableJsonReaderObject GetCollectionFields(string collection, string prefix)
        {
            var operation = new GetCollectionFieldsOperation(collection, prefix);
            var command = operation.CreateRequest();
            if (command == null)
                return null;
            SessionInfo?.IncrementRequestCount();
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            operation.SetResult(command.Result);
            return operation.GetCollectionFields();
        }

        public BlittableJsonReaderObject PreviewCollection(string collection)
        {
            var operation = new PreviewCollectionOperation(collection);
            var command = operation.CreateRequest();
            if (command == null)
                return null;
            SessionInfo?.IncrementRequestCount();
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            operation.SetResult(command.Result);
            return operation.PreviewCollection();
        }
    }

    public class DocumentSessionCollectionOperationsAsync : AdvancedSessionExtensionBase, ICollectionSessionOperationsAsync
    {
        public DocumentSessionCollectionOperationsAsync(AsyncDocumentSession session) : base(session)
        {

        }

        public async Task<BlittableJsonReaderObject> GetCollectionFieldsAsync(string collection, string prefix, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var operation = new GetCollectionFieldsOperation(collection, prefix);
                var command = operation.CreateRequest();
                if (command == null)
                    return null;
                SessionInfo?.IncrementRequestCount();
                await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: SessionInfo, token).ConfigureAwait(false);
                operation.SetResult(command.Result);
                return operation.GetCollectionFields();
            }
        }

        public async Task<BlittableJsonReaderObject> PreviewCollectionAsync(string collection, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var operation = new PreviewCollectionOperation(collection);
                var command = operation.CreateRequest();
                if (command == null)
                    return null;
                SessionInfo?.IncrementRequestCount();
                await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: SessionInfo, token).ConfigureAwait(false);
                operation.SetResult(command.Result);
                return operation.PreviewCollection();
            }
        }
    }
}
