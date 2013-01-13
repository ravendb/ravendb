using System;

namespace Raven.Database.Bundles.SqlReplication
{
	public class LastReplicatedEtag
	{
		public string Name { get; set; }
		public Guid LastDocEtag { get; set; }
	}
}