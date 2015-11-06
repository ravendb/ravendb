using System.ComponentModel;
using Raven.Database.Config.Attributes;

namespace Raven.Database.Config.Categories
{
    public class LicenseConfiguration : ConfigurationCategory
    {
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/License")]
        public string License { get; set; }

        [DefaultValue(null)]
        [ConfigurationEntry("Raven/LicensePath")]
        public string LicensePath { get; set; }

        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Licensing/AllowAdminAnonymousAccessForCommercialUse")]
        public bool AllowAdminAnonymousAccessForCommercialUse { get; set; }
    }
}