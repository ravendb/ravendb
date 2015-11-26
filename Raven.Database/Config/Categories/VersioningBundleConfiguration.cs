using System.ComponentModel;
using Raven.Database.Config.Attributes;

namespace Raven.Database.Config.Categories
{
    public class VersioningBundleConfiguration : ConfigurationCategory
    {
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Versioning/ChangesToRevisionsAllowed")]
        public bool ChangesToRevisionsAllowed { get; set; }
    }
}