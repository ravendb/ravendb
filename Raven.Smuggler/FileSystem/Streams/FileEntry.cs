using Raven.Json.Linq;

namespace Raven.Smuggler.FileSystem.Streams
{
    internal class FileEntry
    {
        public string Key;
        public RavenJObject Metadata;
    }
}