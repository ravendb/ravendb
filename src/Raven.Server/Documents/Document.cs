using Raven.Abstractions.Data;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class Document
    {
        public long Etag;
        public LazyStringValue Key;
        public long StorageId;
        public BlittableJsonReaderObject Data;

        public void EnsureMetadata()
        {
            DynamicJsonValue mutatedMetadata;
            BlittableJsonReaderObject metadata;
            if (Data.TryGet(Constants.Metadata, out metadata))
            {
                metadata.Modifications = mutatedMetadata = new DynamicJsonValue(metadata);
            }
            else
            {
                Data.Modifications = new DynamicJsonValue(Data)
                {
                    [Constants.Metadata] = mutatedMetadata = new DynamicJsonValue()
                };
            }

            mutatedMetadata["@etag"] = Etag;
            mutatedMetadata["@id"] = Key;
        }
    }
}