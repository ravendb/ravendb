using System.IO;
using Raven.Client.Documents.Session;

namespace Raven.Client.Json.Serialization
{
    internal interface IEntityStreamWriter
    {
        bool TryWriteEntityToStream(Stream stream, object entity, IMetadataDictionary metadata, IJsonSerializer serializer);
    }
}
