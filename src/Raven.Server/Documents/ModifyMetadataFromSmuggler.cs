using Raven.Client;
using Raven.Client.Documents.Smuggler;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    class ModifyMetadataFromSmuggler : IMetadataModifier
    {
        private readonly DatabaseItemType _operateOnTypes;

        public ModifyMetadataFromSmuggler(DatabaseItemType operateOnTypes)
        {
            _operateOnTypes = operateOnTypes;
        }

        public DynamicJsonValue ModifyMetadata(BlittableJsonReaderObject metadata, DynamicJsonValue mutatedMetadata)
        {
            if (metadata != null)
            {
                if (_operateOnTypes.HasFlag(DatabaseItemType.Counters) == false && metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray _))
                    mutatedMetadata.Remove(Constants.Documents.Metadata.Counters);
                if (_operateOnTypes.HasFlag(DatabaseItemType.Attachments) == false && metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray _))
                    mutatedMetadata.Remove(Constants.Documents.Metadata.Attachments);
            }

            return mutatedMetadata;
        }
    }
}
