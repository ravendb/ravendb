using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;

namespace Raven.NewClient.Client.Document.Batches
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
