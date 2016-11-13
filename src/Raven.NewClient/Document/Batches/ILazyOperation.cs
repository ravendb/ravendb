using System;
using Raven.Abstractions.Data;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Shard;

namespace Raven.NewClient.Client.Document.Batches
{
    public interface ILazyOperation
    {
        GetRequest CreateRequest();
        object Result { get; }
        QueryResult QueryResult { get; }
        bool RequiresRetry { get; }
        void HandleResponse(GetResponse response);
        void HandleResponses(GetResponse[] responses, ShardStrategy shardStrategy);

        IDisposable EnterContext();
    }
}
