using Raven.Abstractions.Data;

namespace Raven.Abstractions.Smuggler
{
    public class SmugglerFilesOptions : SmugglerOptions<FilesConnectionStringOptions>
    {
        public SmugglerFilesOptions()
        {
            StartFilesEtag = Etag.Empty;
            StartFilesDeletionEtag = Etag.Empty;
        }

        public Etag StartFilesEtag { get; set; }
        public Etag StartFilesDeletionEtag { get; set; }

        public bool StripReplicationInformation { get; set; }

        public bool ShouldDisableVersioningBundle { get; set; }

        /// <summary>
        /// When set ovverides the default document name.
        /// </summary>
        public string NoneDefaultFileName { get; set; }
    }
}
