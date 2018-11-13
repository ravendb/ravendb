using Raven.Client.Documents.Smuggler;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public interface IMetadataModifier
    {
        DatabaseItemType OperateOnTypes { get; set; }
        DynamicJsonValue ModifyMetadata(BlittableJsonReaderObject metadata, DynamicJsonValue mutatedMetadata);
    }
}
