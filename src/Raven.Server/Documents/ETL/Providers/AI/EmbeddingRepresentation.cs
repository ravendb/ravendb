namespace Raven.Server.Documents.ETL.Providers.AI;

public class EmbeddingRepresentation
{
    public string Value { get; set; }
    public float[] EmbeddingValue { get; set; }
    public string ValueHash { get; set; }
    public string AttachmentName { get; set; }
    public string OriginDocumentId { get; set; }
    public string OriginPropertyName { get; set; }
}
