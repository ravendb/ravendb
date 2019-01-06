using Raven.Client;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler.Documents.Data
{
    internal class SmugglerMetadataModifier : IMetadataModifier
    {
        private readonly DatabaseItemType _operateOnTypes;

        public SmugglerMetadataModifier(DatabaseItemType operateOnTypes)
        {
            _operateOnTypes = operateOnTypes;
        }

        public void ModifyMetadata(BlittableJsonReaderObject metadata, ref DynamicJsonValue modifications)
        {
            if (metadata == null)
                return;

            if (_operateOnTypes.HasFlag(DatabaseItemType.CountersBatch) == false && metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray _))
                modifications.Remove(Constants.Documents.Metadata.Counters);

            if (_operateOnTypes.HasFlag(DatabaseItemType.Attachments) == false && metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray _))
                modifications.Remove(Constants.Documents.Metadata.Attachments);
        }
    }
}
