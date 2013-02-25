using System.Collections.Generic;

namespace Raven.Database.Bundles.SqlReplication
{
	public class SqlReplicationStatus
	{
		public List<LastReplicatedEtag> LastReplicatedEtags { get; set; }

		public SqlReplicationStatus()
		{
			LastReplicatedEtags = new List<LastReplicatedEtag>();
		}
	}
}