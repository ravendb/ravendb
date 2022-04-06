using System.Collections.Generic;
using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents;

public class DocumentWithOrderByFields : Document
{
    public List<OrderByField> OrderByFields = new();

    private DocumentWithOrderByFields()
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

    public static DocumentWithOrderByFields From(Document doc)
    {
        return new DocumentWithOrderByFields
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

    public struct OrderByField
    {
        public OrderByFieldType OrderType;
        public string StringValue;
        public long LongValue;
        public double DoubleValue;
    }
}
