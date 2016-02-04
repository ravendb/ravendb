using Raven.Abstractions.Data;

namespace Raven.Abstractions.Database.Smuggler.FileSystem
{
    public class LastFilesEtagsInfo
    {
        public LastFilesEtagsInfo()
        {
            LastFileEtag = Etag.Empty;
            LastDeletedFileEtag = Etag.Empty; // TODO arek - verify if that property make any sense for file systems smuggling
        }

        public Etag LastDeletedFileEtag { get; set; }

        public Etag LastFileEtag { get; set; }
    }
}
