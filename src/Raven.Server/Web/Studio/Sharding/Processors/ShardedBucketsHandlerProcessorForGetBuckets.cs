using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio.Sharding.Processors
{
    internal sealed class ShardedBucketsHandlerProcessorForGetBuckets : AbstractBucketsHandlerProcessorForGetBuckets<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedBucketsHandlerProcessorForGetBuckets([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
        
        protected override async ValueTask<BucketsResults> GetBucketsResults(TransactionOperationContext context, int fromBucket, int toBucket, int range, CancellationToken token)
        {
            var shardNumber = RequestHandler.GetIntValueQueryString(Constants.QueryString.ShardNumber, required: false);
            if (shardNumber.HasValue)
                return await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(new GetBucketsCommand(fromBucket, toBucket, range), shardNumber.Value, token);

            var shardedGetBucketsOperation = new ShardedGetBucketsOperation(RequestHandler.HttpContext.Request, fromBucket, toBucket, range);
            return await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(shardedGetBucketsOperation, token);
        }
    }

    public readonly struct ShardedGetBucketsOperation : IShardedOperation<BucketsResults>
    {
        private readonly HttpRequest _request;
        private readonly int _fromBucket;
        private readonly int _toBucket;
        private readonly int _range;

        public ShardedGetBucketsOperation(HttpRequest httpRequest, int fromBucket, int toBucket, int range)
        {
            _request = httpRequest;
            _fromBucket = fromBucket;
            _toBucket = toBucket;
            _range = range;
        }

        public HttpRequest HttpRequest => _request;

        public BucketsResults Combine(Dictionary<int, ShardExecutionResult<BucketsResults>> results)
        {
            var bucketRanges = new BucketsResults();

            foreach (var shardResult in results.Values)
            {
                var shardStats = shardResult.Result;

                bucketRanges.TotalSize += shardStats.TotalSize;
                
                foreach (var (range, cur) in shardStats.BucketRanges)
                {
                    if (bucketRanges.BucketRanges.TryGetValue(range, out var existing))
                    {
                        existing.DocumentsCount += cur.DocumentsCount;
                        existing.LastModified = existing.LastModified > cur.LastModified ? existing.LastModified : cur.LastModified;
                        existing.RangeSize += cur.RangeSize;
                        existing.NumberOfBuckets += cur.NumberOfBuckets;
                        existing.ShardNumbers.UnionWith(cur.ShardNumbers);
                    }
                    else
                    {
                        bucketRanges.BucketRanges[range] = cur;
                    }
                }
            }

            return bucketRanges;
        }

        public RavenCommand<BucketsResults> CreateCommandForShard(int shardNumber) => new GetBucketsCommand(_fromBucket, _toBucket, _range);
    }
}
