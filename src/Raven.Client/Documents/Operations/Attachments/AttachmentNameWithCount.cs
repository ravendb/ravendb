using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Attachments;

internal sealed class AttachmentNameWithCount : AttachmentName
{
    public long LocalStorageCount { get; set; }
    public long RemoteStorageCount { get; set; }
    public long Count { get; set; }

    internal override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(LocalStorageCount)] = LocalStorageCount;
        json[nameof(RemoteStorageCount)] = RemoteStorageCount;
        json[nameof(Count)] = Count;

        return json;
    }
}
