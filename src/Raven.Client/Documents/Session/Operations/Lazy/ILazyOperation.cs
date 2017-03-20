using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    public interface ILazyOperation
    {
        GetRequest CreateRequest();
        object Result { get; }
        QueryResult QueryResult { get; }
        bool RequiresRetry { get; }
        void HandleResponse(GetResponse response);
    }
}
