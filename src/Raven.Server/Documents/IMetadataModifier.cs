using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public interface IMetadataModifier
    {
        void ModifyMetadata(BlittableJsonReaderObject metadata, ref DynamicJsonValue modifications);
    }
}
