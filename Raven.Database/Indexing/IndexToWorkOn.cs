using System;

namespace Raven.Database.Indexing
{
	public class IndexToWorkOn
	{
		public string IndexName { get; set; }
		public Guid LastIndexedEtag { get; set; }

		public override string ToString()
		{
			return string.Format("IndexName: {0}, LastIndexedEtag: {1}", IndexName, LastIndexedEtag);
		}
	}
}