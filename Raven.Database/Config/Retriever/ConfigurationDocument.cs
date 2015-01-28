using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Config.Retriever
{
	public class ConfigurationDocument<TClass>
		where TClass : class
	{
		public bool LocalExists { get; set; }

		public bool GlobalExists { get; set; }

		public TClass Document { get; set; }

		public Etag Etag { get; set; }

		public RavenJObject Metadata { get; set; }
	}
}