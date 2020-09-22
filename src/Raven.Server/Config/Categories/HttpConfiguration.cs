using System.ComponentModel;
using System.IO.Compression;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;
using Sparrow.Platform;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Http)]
    public class HttpConfiguration : ConfigurationCategory
    {
        public HttpConfiguration()
        {
            Protocols = PlatformDetails.CanUseHttp2 ? HttpProtocols.Http1AndHttp2 : HttpProtocols.Http1;
        }

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

        [Description("Set Kestrel's MaxRequestLineSize")]
        [DefaultValue(16)]
        [SizeUnit(SizeUnit.Kilobytes)]
        [ConfigurationEntry("Http.MaxRequestLineSizeInKb", ConfigurationEntryScope.ServerWideOnly)]
        public Size MaxRequestLineSize { get; set; }

        [Description("Whether Raven's HTTP server should compress its responses")]
        [DefaultValue(true)]
        [ConfigurationEntry("Http.UseResponseCompression", ConfigurationEntryScope.ServerWideOnly)]
        public bool UseResponseCompression { get; set; }

        [Description("Whether Raven's HTTP server should allow response compression to happen when HTTPS is enabled. Please see http://breachattack.com/ before enabling this")]
        [DefaultValue(false)]
        [ConfigurationEntry("Http.AllowResponseCompressionOverHttps", ConfigurationEntryScope.ServerWideOnly)]
        public bool AllowResponseCompressionOverHttps { get; set; }

        [Description("Compression level to be used when compressing HTTP responses with GZip")]
        [DefaultValue(CompressionLevel.Fastest)]
        [ConfigurationEntry("Http.GzipResponseCompressionLevel", ConfigurationEntryScope.ServerWideOnly)]
        public CompressionLevel GzipResponseCompressionLevel { get; set; }

        [Description("Compression level to be used when compressing HTTP responses with Deflate")]
        [DefaultValue(CompressionLevel.Fastest)]
        [ConfigurationEntry("Http.DeflateResponseCompressionLevel", ConfigurationEntryScope.ServerWideOnly)]
        public CompressionLevel DeflateResponseCompressionLevel { get; set; }

        [Description("Compression level to be used when compressing static files")]
        [DefaultValue(CompressionLevel.Optimal)]
        [ConfigurationEntry("Http.StaticFilesResponseCompressionLevel", ConfigurationEntryScope.ServerWideOnly)]
        public CompressionLevel StaticFilesResponseCompressionLevel { get; set; }

        [Description("Sets HTTP protocols that should be supported by the server")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [ConfigurationEntry("Http.Protocols", ConfigurationEntryScope.ServerWideOnly)]
        public HttpProtocols Protocols { get; set; }
    }
}
