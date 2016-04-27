using System;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Shard;

namespace Raven.Client.Document.Batches
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
