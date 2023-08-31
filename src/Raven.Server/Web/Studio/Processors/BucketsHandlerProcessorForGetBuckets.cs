using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Studio.Processors
{
    internal sealed class BucketsHandlerProcessorForGetBuckets : AbstractBucketsHandlerProcessorForGetBuckets<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public BucketsHandlerProcessorForGetBuckets([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
        
        protected override ValueTask<BucketsResults> GetBucketsResults(DocumentsOperationContext context, int fromBucket, int toBucket, int range, CancellationToken token)
        {
            if (ShardHelper.TryGetShardNumberAndDatabaseName(RequestHandler.DatabaseName, out string shardedDatabaseName, out int shardNumber) == false)
            {
                throw new InvalidOperationException($"Attempted to get buckets of a shard. Expected a shard database but instead got {RequestHandler.Database}");
            }

            using (context.OpenReadTransaction())
            {
                long shardBucketsSize = 0;
                var ranges = new Dictionary<int, BucketRange>();
                foreach (var bucket in ShardedDocumentsStorage.GetBucketStatistics(context, fromBucket, toBucket))
                {
                    var id = (bucket.Bucket / range) * range;
                    if (ranges.TryGetValue(id, out var existing))
                    {
                        existing.RangeSize += bucket.Size;
                        existing.DocumentsCount += bucket.NumberOfDocuments;
                        existing.LastModified = existing.LastModified > bucket.LastModified ? existing.LastModified : bucket.LastModified;
                        existing.NumberOfBuckets++;

                    }
                    else
                    {
                        var bucketRange = new BucketRange()
                        {
                            ShardNumbers = { shardNumber },
                            RangeSize = bucket.Size,
                            FromBucket = id,
                            NumberOfBuckets = 1,
                            ToBucket = id + range - 1,
                            LastModified = bucket.LastModified,
                            DocumentsCount = bucket.NumberOfDocuments
                        };
                        
                        ranges[id] = bucketRange;
                    }

                    shardBucketsSize += bucket.Size;
                }

                return ValueTask.FromResult(new BucketsResults()
                {
                    BucketRanges = ranges,
                    TotalSize = shardBucketsSize
                });
            }
        }
    }

    public sealed class BucketRange : IDynamicJson
    {
        public long FromBucket;
        public long ToBucket;
        public long NumberOfBuckets;
        public long RangeSize;
        public HashSet<int> ShardNumbers = new();
        
        public string RangeSizeHumane => Size.Humane(RangeSize);
        public long DocumentsCount;
        public DateTime LastModified;

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue()
            {
                [nameof(RangeSizeHumane)] = RangeSizeHumane,
                [nameof(ShardNumbers)] = new DynamicJsonArray(ShardNumbers),
                [nameof(FromBucket)] = FromBucket,
                [nameof(ToBucket)] = ToBucket,
                [nameof(NumberOfBuckets)] = NumberOfBuckets,
                [nameof(RangeSize)] = RangeSize,
                [nameof(DocumentsCount)] = DocumentsCount,
                [nameof(LastModified)] = LastModified,
            };
            
            return json;
        }
    }

    public sealed class BucketsResults
    {
        public long TotalSize;
        public string TotalSizeHumane => Size.Humane(TotalSize);
        public Dictionary<int, BucketRange> BucketRanges = new();

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(TotalSizeHumane)] = TotalSizeHumane,
                [nameof(TotalSize)] = TotalSize,
            };

            var ranges = new DynamicJsonValue();
            foreach (var range in BucketRanges)
            {
                ranges[range.Key.ToString()] = range.Value.ToJson();
            }

            json[nameof(BucketRanges)] = ranges;

            return json;
        }
    }
}
