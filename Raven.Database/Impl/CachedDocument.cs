using Raven.Json.Linq;

namespace Raven.Database.Impl
{
	public class CachedDocument
	{
		public int Size { get; set; }
		public RavenJObject Metadata { get; set; }
		public RavenJObject Document { get; set; }
	}
}