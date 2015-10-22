using System;
using Raven.Abstractions.Data;

namespace Raven.Database.Indexing
{
	public class IndexToWorkOn
	{
		public int IndexId { get; set; }
		public Etag LastIndexedEtag { get; set; }
		public DateTime LastIndexedTimestamp { get; set; }

		public Index Index { get; set; }

		public override string ToString()
		{
			return string.Format("Index: {0}, LastIndexedEtag: {1}", Index == null ? IndexId.ToString() : Index.PublicName, LastIndexedEtag);
		}
	}
}