using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Queries;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    public interface ILazyOperation
    {
        GetRequest CreateRequest(JsonOperationContext ctx);
        object Result { get; }
        QueryResult QueryResult { get; }
        bool RequiresRetry { get; }
        void HandleResponse(GetResponse response);
    }
}
