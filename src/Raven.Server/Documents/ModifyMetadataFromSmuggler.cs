using Raven.Client;
using Raven.Client.Documents.Smuggler;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    class ModifyMetadataFromSmuggler : IMetadataModifier
    {
        public DatabaseItemType OperateOnTypes { get; set; }

        public ModifyMetadataFromSmuggler(DatabaseItemType operateOnTypes)
        {
            OperateOnTypes = operateOnTypes;
        }

        public DynamicJsonValue ModifyMetadata(BlittableJsonReaderObject metadata, DynamicJsonValue mutatedMetadata)
        {
            if (metadata != null)
            {
                if (OperateOnTypes.HasFlag(DatabaseItemType.Counters) == false && metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray _))
                    mutatedMetadata.Remove(Constants.Documents.Metadata.Counters);
                if (OperateOnTypes.HasFlag(DatabaseItemType.Attachments) == false && metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray _))
                    mutatedMetadata.Remove(Constants.Documents.Metadata.Attachments);
            }

            return mutatedMetadata;
        }
    }
}
