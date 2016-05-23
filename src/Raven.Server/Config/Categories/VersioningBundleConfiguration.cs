using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class VersioningBundleConfiguration : ConfigurationCategory
    {
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Versioning/ChangesToRevisionsAllowed")]
        public bool ChangesToRevisionsAllowed { get; set; }
    }
}