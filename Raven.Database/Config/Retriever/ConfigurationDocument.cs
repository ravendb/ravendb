using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Config.Retriever
{
	public class ConfigurationDocument<TClass>
	{
		public bool LocalExists { get; set; }

		public bool GlobalExists { get; set; }

		public TClass MergedDocument { get; set; }

        public TClass GlobalDocument { get; set; }

		public Etag Etag { get; set; }

		public RavenJObject Metadata { get; set; }
	}
}