using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Attachments;

internal sealed class AttachmentNameWithCount : AttachmentName
{
    public long RegularHashes { get; set; }
    public long RetiredCount { get; set; }
    public long Count { get; set; }

    internal override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(RegularHashes)] = RegularHashes;
        json[nameof(RetiredCount)] = RetiredCount;
        json[nameof(Count)] = Count;

        return json;
    }
}