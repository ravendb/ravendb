using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents.Sharding.Queries;

public class ShardedQueryResultDocument : Document
{
    public List<OrderByField> OrderByFields = new();

    public ulong? DistinctDataHash { get; set; }

    private ShardedQueryResultDocument()
    {
    }

    public void AddStringOrderByField(string value)
    {
        OrderByFields.Add(new OrderByField
        {
            OrderType = OrderByFieldType.String,
            StringValue = value
        });
    }

    public void AddLongOrderByField(long value)
    {
        OrderByFields.Add(new OrderByField
        {
            OrderType = OrderByFieldType.Long,
            LongValue = value
        });
    }

    public void AddDoubleOrderByField(double value)
    {
        OrderByFields.Add(new OrderByField
        {
            OrderType = OrderByFieldType.Double,
            DoubleValue = value
        });
    }

    public static ShardedQueryResultDocument From(Document doc)
    {
        return new ShardedQueryResultDocument
        {
            Etag = doc.Etag,
            StorageId = doc.StorageId,
            IndexScore = doc.IndexScore,
            Distance = doc.Distance,
            ChangeVector = doc.ChangeVector,
            LastModified = doc.LastModified,
            Flags = doc.Flags,
            NonPersistentFlags = doc.NonPersistentFlags,
            TransactionMarker = doc.TransactionMarker,
            Id = doc.Id,
            LowerId = doc.LowerId,
            Data = doc.Data
        };
    }

    public static bool ShouldAddShardingSpecificMetadata(IndexQueryServerSide query, IndexType indexType, out (bool OrderByFields, bool DistinctDataHash) shouldAdd)
    {
        // * for sharded queries, we'll send the order by fields separately
        // * for a map-reduce index, it's fields are the ones that are used for sorting
        if (query.Metadata.OrderBy?.Length > 0 == false || indexType.IsMapReduce())
            shouldAdd.OrderByFields = false;
        else
            shouldAdd.OrderByFields = true;


        shouldAdd.DistinctDataHash = query.Metadata.IsDistinct;

        return shouldAdd.OrderByFields | shouldAdd.DistinctDataHash;
    }

    public struct OrderByField
    {
        public OrderByFieldType OrderType;
        public string StringValue;
        public long LongValue;
        public double DoubleValue;
    }
}
