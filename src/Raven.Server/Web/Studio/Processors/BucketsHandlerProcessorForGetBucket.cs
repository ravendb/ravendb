using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.Studio.Processors
{
    internal class BucketsHandlerProcessorForGetBucket : AbstractBucketsHandlerProcessorForGetBucket<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public BucketsHandlerProcessorForGetBucket([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
        
        protected override ValueTask<BucketInfo> GetBucketInfo(DocumentsOperationContext context, int bucket)
        {
            using (var tx = context.OpenReadTransaction())
            {
                var shardedDocumentDatabase = ShardedDocumentDatabase.CastToShardedDocumentDatabase(RequestHandler.Database);
                var stats = new ReplicationDocumentSenderBase.ReplicationStats();
                using var helper = new DocumentInfoHelper(context);

                var items = new List<string>();
                foreach (var item in MigrationReplicationDocumentSender.ReplicationBatchItemsForBucket(shardedDocumentDatabase.ShardedDocumentsStorage, context, 0, stats, bucket))
                {
                    var info = helper.GetItemInformation(item); 
                    items.Add(info);
                }

                var bucketStats = ShardedDocumentsStorage.GetBucketStatisticsFor(context, bucket);

                var streamInfo = shardedDocumentDatabase.ShardedDocumentsStorage.AttachmentsStorage.GetStreamInfoForBucket(tx.InnerTransaction, bucket);
                if (streamInfo.UniqueAttachmets > 0)
                {
                    items.Add($"Has {streamInfo.UniqueAttachmets} unique stream with total size of {new Size(streamInfo.TotalSize).HumaneSize}");
                }
                if (bucketStats == null)
                {
                    return ValueTask.FromResult(new BucketInfo()
                    {
                        Bucket = bucket,
                        Items = items
                    });
                }

                var bucketInfo = new BucketInfo()
                {
                    Bucket = bucket,
                    Size = bucketStats.Size,
                    NumberOfDocuments = bucketStats.NumberOfDocuments,
                    LastModified = bucketStats.LastModified,
                    Items = items
                };

                return ValueTask.FromResult(bucketInfo);
            }
        }
    }
}
