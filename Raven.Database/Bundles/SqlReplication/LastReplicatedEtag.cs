using System;
using Raven.Abstractions.Data;

namespace Raven.Database.Bundles.SqlReplication
{
	public class LastReplicatedEtag
	{
		public string Name { get; set; }
		public Etag LastDocEtag { get; set; }
	}
}