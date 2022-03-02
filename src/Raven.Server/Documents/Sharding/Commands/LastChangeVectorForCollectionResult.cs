namespace Raven.Server.Documents.Sharding.Commands;

public class LastChangeVectorForCollectionResult
{
    public string Collection { get; set; }
    public string LastChangeVector { get; set; }
}
