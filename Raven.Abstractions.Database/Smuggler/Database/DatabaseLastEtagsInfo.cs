using Raven.Abstractions.Data;

namespace Raven.Abstractions.Database.Smuggler.Database
{
	public class DatabaseLastEtagsInfo
	{
		public DatabaseLastEtagsInfo()
		{
			LastDocsEtag = Etag.Empty;
			LastDocDeleteEtag = Etag.Empty;
		}
		public Etag LastDocsEtag { get; set; }
		public Etag LastDocDeleteEtag { get; set; }
	}
}