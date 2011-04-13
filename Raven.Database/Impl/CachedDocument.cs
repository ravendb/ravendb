using Raven.Json.Linq;

namespace Raven.Database.Impl
{
    public class CachedDocument
    {
        public RavenJObject Metadata { get; set; }
        public RavenJObject Document { get; set; }
    }
}