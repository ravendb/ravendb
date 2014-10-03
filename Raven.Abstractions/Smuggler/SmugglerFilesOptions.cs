using Raven.Abstractions.Data;

namespace Raven.Abstractions.Smuggler
{
    public class SmugglerFilesOptions : SmugglerOptions<FilesConnectionStringOptions>
    {
        public SmugglerFilesOptions()
        {
            this.StartFilesEtag = Etag.Empty;
            this.StartFilesDeletionEtag = Etag.Empty;
        }

        public Etag StartFilesEtag { get; set; }
        public Etag StartFilesDeletionEtag { get; set; }
    }
}