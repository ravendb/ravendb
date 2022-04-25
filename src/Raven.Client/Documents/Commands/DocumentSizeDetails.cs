using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands;

internal class DocumentSizeDetails : IDynamicJson
{
    public string DocId { get; set; }
    public int ActualSize { get; set; }
    public string HumaneActualSize { get; set; }
    public int AllocatedSize { get; set; }
    public string HumaneAllocatedSize { get; set; }

    public virtual DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(DocId)] = DocId,
            [nameof(ActualSize)] = ActualSize,
            [nameof(HumaneActualSize)] = HumaneActualSize,
            [nameof(AllocatedSize)] = AllocatedSize,
            [nameof(HumaneAllocatedSize)] = HumaneAllocatedSize
        };
    }
}
