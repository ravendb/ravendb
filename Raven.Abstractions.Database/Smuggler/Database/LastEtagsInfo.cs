using System;

using Raven.Abstractions.Data;

namespace Raven.Abstractions.Smuggler.Data
{
	public class LastEtagsInfo
	{
		public LastEtagsInfo()
		{
			LastDocsEtag = Etag.Empty;
			LastDocDeleteEtag = Etag.Empty;
		}
		public Etag LastDocsEtag { get; set; }
		public Etag LastDocDeleteEtag { get; set; }
	}

    public class LastFilesEtagsInfo
    {
        public LastFilesEtagsInfo()
        {
            this.LastFileEtag = Etag.Empty;
            this.LastDeletedFileEtag = Etag.Empty;
        }

        public Etag LastDeletedFileEtag { get; set; }

        public Etag LastFileEtag { get; set; }
    }

}