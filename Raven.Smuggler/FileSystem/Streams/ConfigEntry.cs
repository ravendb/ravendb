using Raven.Json.Linq;

namespace Raven.Smuggler.FileSystem.Streams
{
    internal class ConfigEntry
    {
        public string Name;
        public RavenJObject Value;
    }
}