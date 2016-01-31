using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class LicenseConfiguration : ConfigurationCategory
    {
        [Description("The full license string for RavenDB. If Raven/License is specified, it overrides the Raven/LicensePath configuration.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/License")]
        public string License { get; set; }

        [Description("The path to the license file for RavenDB, default for ~\\license.xml")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/LicensePath")]
        public string LicensePath { get; set; }

        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Licensing/AllowAdminAnonymousAccessForCommercialUse")]
        public bool AllowAdminAnonymousAccessForCommercialUse { get; set; }
    }
}