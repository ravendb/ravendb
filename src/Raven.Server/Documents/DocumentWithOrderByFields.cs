using System.Collections.Generic;

namespace Raven.Server.Documents;

public class DocumentWithOrderByFields : Document
{
    public List<(string Field, string Value)> OrderByFields;

    public void AddOrderByField(string fieldName, string value)
    {
        OrderByFields ??= new List<(string Field, string Value)>();
        OrderByFields.Add((fieldName, value));
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
