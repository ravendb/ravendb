using Raven.Json.Linq;
using Raven.Abstractions.Data;

namespace Raven.Database.FileSystem.Storage
{
    public class RenameFileOperation
    {
        public RenameFileOperation(string name, string rename, Etag currentEtag, RavenJObject metadataAfterOperation)
        {
            Name = name;
            Rename = rename;
            Etag = currentEtag;
            MetadataAfterOperation = metadataAfterOperation;
        }

        public string Name { get; private set; }

        public string Rename { get; private set; }

        public Etag Etag { get; private set; }

        public RavenJObject MetadataAfterOperation { get; private set; }

        public bool ForceExistingFileRemoval { get; set; }
    }
}
