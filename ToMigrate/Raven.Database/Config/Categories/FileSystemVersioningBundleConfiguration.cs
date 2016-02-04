using System.ComponentModel;
using Raven.Database.Config.Attributes;

namespace Raven.Database.Config.Categories
{
    public class FileSystemVersioningBundleConfiguration : ConfigurationCategory
    {
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/FileSystem/Versioning/ChangesToRevisionsAllowed")]
        public bool ChangesToRevisionsAllowed { get; set; }
    }
}