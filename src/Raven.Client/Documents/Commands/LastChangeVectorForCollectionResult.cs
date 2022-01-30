namespace Raven.Client.Documents.Commands;

public class LastChangeVectorForCollectionResult
{
    public string Collection { get; set; }
    public string LastChangeVector { get; set; }
}
