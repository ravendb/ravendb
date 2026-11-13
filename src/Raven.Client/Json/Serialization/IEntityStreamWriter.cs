using System.IO;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization
{
    internal interface IEntityStreamWriter
    {
        bool TryWriteEntityToStream(Stream stream, object entity, IMetadataDictionary metadata, IJsonSerializer serializer, JsonOperationContext context);
    }
}
