using Raven.Client.Data;
using Raven.Client.Data.Queries;

namespace Raven.Client.Document.Batches
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
