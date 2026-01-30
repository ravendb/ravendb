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
    public sealed class HttpConfiguration : ConfigurationCategory
    {
        public HttpConfiguration()
        {
            Http2Profile = Http2Profile.Balanced;
            Protocols = PlatformDetails.CanUseHttp2 ? HttpProtocols.Http1AndHttp2 : HttpProtocols.Http1;
            
            // Prefer lower memory defaults on 32-bit runtimes to avoid address space pressure.
            if (PlatformDetails.Is32Bits)
                Http2Profile = Http2Profile.Conservative;
        }

        [Description("Set Kestrel's minimum required data rate in bytes per second. This option must be configured together with 'Http.MinDataRateGracePeriod'.")]
        [DefaultValue(null)]
        [SizeUnit(SizeUnit.Bytes)]
        [ConfigurationEntry("Http.MinDataRateBytesPerSec", ConfigurationEntryScope.ServerWideOnly)]
        public Size? MinDataRatePerSecond { get; set; }

        [Description("Set Kestrel's allowed request and response grace in seconds. This option must be configured together with 'Http.MinDataRateBytesPerSec'.")]
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

        [Description("Set Kestrel's HTTP2 keep alive ping timeout")]
        [DefaultValue(null)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Http.Http2.KeepAlivePingTimeoutInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting? KeepAlivePingTimeout { get; set; }

        [Description("Set Kestrel's HTTP2 keep alive ping delay")]
        [DefaultValue(null)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Http.Http2.KeepAlivePingDelayInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting? KeepAlivePingDelay { get; set; }

        [Description("Set Kestrel's HTTP2 max streams per connection. This limits the number of concurrent request streams per HTTP/2 connection. Excess streams will be refused.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Http.Http2.MaxStreamsPerConnection", ConfigurationEntryScope.ServerWideOnly)]
        public int? MaxStreamsPerConnection { get; set; }

        [Description("Set whether Raven's HTTP server should compress its responses")]
        [DefaultValue(true)]
        [ConfigurationEntry("Http.UseResponseCompression", ConfigurationEntryScope.ServerWideOnly)]
        public bool UseResponseCompression { get; set; }

        [Description("Set whether Raven's HTTP server should allow response compression to happen when HTTPS is enabled.")]
        [DefaultValue(true)]
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

#if FEATURE_BROTLI_SUPPORT
        [Description("Compression level to be used when compressing HTTP responses with Brotli")]
        [DefaultValue(CompressionLevel.Optimal)]
        [ConfigurationEntry("Http.BrotliResponseCompressionLevel", ConfigurationEntryScope.ServerWideOnly)]
        public CompressionLevel BrotliResponseCompressionLevel { get; set; }
#endif

        [Description("Compression level to be used when compressing HTTP responses with Zstd")]
        [DefaultValue(CompressionLevel.Fastest)]
        [ConfigurationEntry("Http.ZstdResponseCompressionLevel", ConfigurationEntryScope.ServerWideOnly)]
        public CompressionLevel ZstdResponseCompressionLevel { get; set; }

        [Description("Compression level to be used when compressing static files")]
        [DefaultValue(CompressionLevel.Optimal)]
        [ConfigurationEntry("Http.StaticFilesResponseCompressionLevel", ConfigurationEntryScope.ServerWideOnly)]
        public CompressionLevel StaticFilesResponseCompressionLevel { get; set; }

        [Description("Set HTTP protocols that should be supported by the server")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [ConfigurationEntry("Http.Protocols", ConfigurationEntryScope.ServerWideOnly)]
        public HttpProtocols Protocols { get; set; }

        [Description("Set a value that controls whether synchronous IO is allowed for the Request and Response")]
        [DefaultValue(false)]
        [ConfigurationEntry("Http.AllowSynchronousIO", ConfigurationEntryScope.ServerWideOnly)]
        public bool AllowSynchronousIo { get; set; }

        [Description($"HTTP/2 performance profile: {nameof(Http2Profile.Performance)} | {nameof(Http2Profile.Balanced)} | {nameof(Http2Profile.Conservative)}. {nameof(Http2Profile.Performance)}=max throughput (more memory). {nameof(Http2Profile.Balanced)}=balanced. {nameof(Http2Profile.Conservative)}=lower memory (may cap throughput on high RTT).")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [ConfigurationEntry("Http.Http2.Profile", ConfigurationEntryScope.ServerWideOnly)]
        public Http2Profile Http2Profile { get; set; }

        [Description($"Latency hint influencing profile sizing: {nameof(Http2LatencyHint.Default)} | {nameof(Http2LatencyHint.High)}. {nameof(Http2LatencyHint.Default)}=use profile as-is for low RTT (collocated: same region/AZ). {nameof(Http2LatencyHint.High)}=2x windows for WAN/high RTT unless explicit sizes are set.")]
        [DefaultValue(Http2LatencyHint.Default)]
        [ConfigurationEntry("Http.Http2.LatencyHint", ConfigurationEntryScope.ServerWideOnly)]
        public Http2LatencyHint Http2Latency { get; set; }

        [Description("EXPERT: Override Kestrel HTTP/2 InitialConnectionWindowSize in KB (per-connection receive window). Prefer Profile/LatencyHint unless you understand your BDP and memory tradeoffs. See RFC-9113 for details.")]
        [DefaultValue(null)]
        [SizeUnit(SizeUnit.Kilobytes)]
        [ConfigurationEntry("Http.Http2.InitialConnectionWindowSizeInKb", ConfigurationEntryScope.ServerWideOnly)]
        public Size? InitialConnectionWindowSize { get; set; }

        [Description("EXPERT: Override Kestrel HTTP/2 InitialStreamWindowSize in KB (per-stream receive window). Prefer Profile/LatencyHint unless tuning for specific concurrency/RTT. See RFC-9113 for details.")]
        [DefaultValue(null)]
        [SizeUnit(SizeUnit.Kilobytes)]
        [ConfigurationEntry("Http.Http2.InitialStreamWindowSizeInKb", ConfigurationEntryScope.ServerWideOnly)]
        public Size? InitialStreamWindowSize { get; set; }

        [Description("EXPERT: Override Kestrel HTTP/2 MaxFrameSize in KB (maximum payload per frame, 16KB-16MB). Larger frames reduce overhead for bulk transfers but delay small urgent frames. See RFC-9113 Section 4.2.")]
        [DefaultValue(null)]
        [SizeUnit(SizeUnit.Kilobytes)]
        [ConfigurationEntry("Http.Http2.MaxFrameSizeInKb", ConfigurationEntryScope.ServerWideOnly)]
        public Size? MaxFrameSize { get; set; }
    }

    public enum Http2Profile
    {
        /// <summary>
        /// Favor peak throughput: large HTTP/2 flow-control windows to keep high-BDP links full. Uses more memory per connection.
        /// </summary>
        Performance,
        /// <summary>
        /// Balanced defaults for most deployments: good throughput without excessive memory usage.
        /// </summary>
        Balanced,
        /// <summary>
        /// Favor lower memory usage: smaller windows. May limit throughput on high-latency or high-bandwidth links.
        /// </summary>
        Conservative
    }

    public enum Http2LatencyHint
    {
        /// <summary>
        /// Low latency. Server and clients are collocated (same region/AZ) or on a LAN/Cluster.
        /// Keeps the selected profile as-is. Choose this when round-trip times are short
        /// and HTTP/2 throughput is not flattening due to flow control.
        /// </summary>
        Default,
        /// <summary>
        /// High latency. Cross-region or internet links where round-trip times are long.
        /// Doubles the HTTP/2 flow-control windows derived from the selected profile to
        /// maintain smooth throughput over larger RTTs. Use when downloads plateau under HTTP/2
        /// and improve after increasing window sizes or when forcing HTTP/1.1.
        /// </summary>
        High
    }
}
