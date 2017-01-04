using System;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Shard;
using Sparrow.Json;

namespace Raven.NewClient.Client.Document.Batches
{
    public interface ILazyOperation
    {
        GetRequest CreateRequest();
        object Result { get; }
        QueryResult QueryResult { get; }
        bool RequiresRetry { get; }
        void HandleResponse(BlittableJsonReaderObject response);
    }
}
