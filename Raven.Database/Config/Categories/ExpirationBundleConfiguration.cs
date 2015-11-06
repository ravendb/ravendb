using System.ComponentModel;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;

namespace Raven.Database.Config.Categories
{
    public class ExpirationBundleConfiguration : ConfigurationCategory
    {
        [DefaultValue(300)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Expiration/DeleteFrequencyInSec")]
        [ConfigurationEntry("Raven/Expiration/DeleteFrequencySeconds")]
        public TimeSetting DeleteFrequency { get; set; }
    }
}