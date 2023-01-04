using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using NotImplementedException = System.NotImplementedException;

namespace Raven.Server.Web.Studio.Processors
{
    internal class BucketsHandlerProcessorForGetBucket : AbstractBucketsHandlerProcessorForGetBucket<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public BucketsHandlerProcessorForGetBucket([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
        
        protected override ValueTask<BucketInfo> GetBucketInfo(DocumentsOperationContext context, int bucket)
        {
            using (context.OpenReadTransaction())
            {
                var database = ShardedDocumentDatabase.CastToShardedDocumentDatabase(RequestHandler.Database);
                var docs = database.ShardedDocumentsStorage.GetDocumentsByBucketFrom(context, bucket, etag: 0);
                var bucketStats = ShardedDocumentsStorage.GetBucketStatisticsFor(context, bucket);

                if (bucketStats == null)
                {
                    return ValueTask.FromResult(new BucketInfo()
                    {
                        Bucket = bucket,
                        Documents = docs.Select(d => d.Id.ToString()).ToList()
                    });
                }

                var bucketInfo = new BucketInfo()
                {
                    Bucket = bucket,
                    Size = bucketStats.Size,
                    NumberOfDocuments = bucketStats.NumberOfDocuments,
                    LastModified = bucketStats.LastModified,
                    Documents = docs.Select(d => d.Id.ToString()).ToList()
                };

                return ValueTask.FromResult(bucketInfo);
            }
        }
    }
}
