using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio.Sharding.Processors
{
    internal sealed class ShardedBucketsHandlerProcessorForGetBucket : AbstractBucketsHandlerProcessorForGetBucket<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedBucketsHandlerProcessorForGetBucket([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
        protected override async ValueTask<BucketInfo> GetBucketInfo(TransactionOperationContext context, int bucket)
        {
            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                var shardNumber = ShardHelper.GetShardNumberFor(RequestHandler.DatabaseContext.DatabaseRecord.Sharding, bucket);
                var cmd = new GetBucketInfoCommand(bucket);
                return await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber, token.Token);
            }
        }
    }
}
