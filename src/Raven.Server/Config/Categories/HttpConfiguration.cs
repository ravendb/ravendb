using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;

namespace Raven.Server.Config.Categories
{
    public class HttpConfiguration : ConfigurationCategory
    {
        [Description("Set Kestrel's minimum required data rate in bytes per second. This option should configured together with 'Http.MinDataRateGracePeriod'")]
        [DefaultValue(null)]
        [SizeUnit(SizeUnit.Bytes)]
        [ConfigurationEntry("Http.MinDataRateBytesPerSec", ConfigurationEntryScope.ServerWideOnly)]
        public Size? MinDataRatePerSecond { get; set; }

        [Description("Set Kestrel's allowed request and reponse grace in seconds. This option should configured together with 'Http.MinDataRateBytesPerSec'")]
        [DefaultValue(null)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Http.MinDataRateGracePeriodInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting? MinDataRateGracePeriod { get; set; }

        [Description("Set Kestrel's MaxRequestBufferSize")]
        [DefaultValue(null)]
        [SizeUnit(SizeUnit.Kilobytes)]
        [ConfigurationEntry("Http.MaxRequestBufferSizeInKb", ConfigurationEntryScope.ServerWideOnly)]
        public Size? MaxRequestBufferSize { get; set; }

        [Description("Whether Raven's HTTP server should GZip encode its responses")]
        [DefaultValue(true)]
        [ConfigurationEntry("Http.AllowResponseCompression", ConfigurationEntryScope.ServerWideOnly)]
        public bool AllowResponseCompression { get; set; }
    }
}
