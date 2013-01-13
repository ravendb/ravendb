using System.Collections.Generic;

namespace Raven.Database.Bundles.SqlReplication
{
	public class SqlReplicationStatus
	{
		public string Id { get; set; }
		public List<LastReplicatedEtag> LastReplicatedEtags { get; set; }

		public SqlReplicationStatus()
		{
			LastReplicatedEtags = new List<LastReplicatedEtag>();
		}
	}
}