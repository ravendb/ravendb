using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.CdcSink)]
    public class CdcSinkConfiguration : ConfigurationCategory
    {
        [Description("Max number of pulled messages consumed in a single batch")]
        [DefaultValue(8192)]
        [ConfigurationEntry("CdcSink.MaxBatchSize", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int? MaxBatchSize { get; protected set; }

        [Description("Maximum number of seconds CDC Sink process will be in a fallback mode after a connection failure. The fallback mode means suspending the process.")]
        [DefaultValue(60 * 15)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("CdcSink.MaxFallbackTimeInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MaxFallbackTime { get; set; }
    }
}
