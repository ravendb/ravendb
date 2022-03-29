using System.Collections.Generic;
using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents;

public class DocumentWithOrderByFields : Document
{
    public List<(string Field, OrderByFieldType OrderType, object Value)> OrderByFields;

    private DocumentWithOrderByFields()
    {
    }

    public void AddOrderByField(string fieldName, OrderByFieldType orderType, object value)
    {
        OrderByFields ??= new List<(string Field, OrderByFieldType OrderType, object Value)>();
        OrderByFields.Add((fieldName, orderType, value));
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
}
