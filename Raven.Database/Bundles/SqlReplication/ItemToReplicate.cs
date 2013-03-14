using Raven.Json.Linq;

namespace Raven.Database.Bundles.SqlReplication
{
	public class ItemToReplicate
	{
		public string DocumentId { get; set; }
		public RavenJObject Columns { get; set; }
	}
}