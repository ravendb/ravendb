using Raven.Abstractions.Data;

namespace Raven.Abstractions.Database.Smuggler.Other
{
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
