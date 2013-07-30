using Raven.Abstractions.Data;

namespace Raven.Database.Indexing
{
	public class IndexToWorkOn
	{
		public int IndexId { get; set; }
		public Etag LastIndexedEtag { get; set; }

		public Index Index { get; set; }

		public override string ToString()
		{
			return string.Format("IndexId: {0}, LastIndexedEtag: {1}", IndexId, LastIndexedEtag);
		}
	}
}