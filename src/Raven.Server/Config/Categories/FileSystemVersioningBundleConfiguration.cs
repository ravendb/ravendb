using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class FileSystemVersioningBundleConfiguration : ConfigurationCategory
    {
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/FileSystem/Versioning/ChangesToRevisionsAllowed")]
        public bool ChangesToRevisionsAllowed { get; set; }
    }
}