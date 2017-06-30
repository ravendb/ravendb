using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public class ExpirationConfiguration : ConfigurationCategory
    {
        [DefaultValue(300)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Expiration/DeleteFrequencyInSec")]
        public TimeSetting DeleteFrequency { get; set; }
    }
}