using System.ComponentModel;
using Raven.Database.Config.Attributes;

namespace Raven.Database.Config.Categories
{
    public class QuotasBundleConfiguration : ConfigurationCategory
    {
        [DefaultValue(long.MaxValue)]
        [ConfigurationEntry("Raven/Quotas/Documents/HardLimit")]
        public long DocsHardLimit { get; set; }

        [DefaultValue(long.MaxValue)]
        [ConfigurationEntry("Raven/Quotas/Documents/SoftLimit")]
        public long DocsSoftLimit { get; set; }

        [Description("The hard limit after which we refuse any additional writes")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/Quotas/Size/HardLimitInKB")]
        public string SizeHardLimit { get; set; }

        [Description("The soft limit before which we will warn about the quota")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/Quotas/Size/SoftMarginInKB")]
        public string SizeSoftLimit { get; set; }
    }
}