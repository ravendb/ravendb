using System;

using Raven.Abstractions.Data;

namespace Raven.Abstractions.Smuggler.Data
{
	public class LastEtagsInfo
	{
		public LastEtagsInfo()
		{
			LastDocsEtag = Etag.Empty;
			LastAttachmentsEtag = Etag.Empty;
			LastDocDeleteEtag = Etag.Empty;
			LastAttachmentsDeleteEtag = Etag.Empty;
		}
		public Etag LastDocsEtag { get; set; }
		public Etag LastDocDeleteEtag { get; set; }

        [Obsolete("Use RavenFS instead.")]
		public Etag LastAttachmentsEtag { get; set; }

        [Obsolete("Use RavenFS instead.")]
		public Etag LastAttachmentsDeleteEtag { get; set; }
	}
}