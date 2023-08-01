using System.ComponentModel;
using System.IO.Compression;
using System.Threading;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Sharding)]
    public sealed class ShardingConfiguration : ConfigurationCategory
    {
        public ShardingConfiguration()
        {
            OrchestratorTimeout = new TimeSetting(Timeout.InfiniteTimeSpan);
        }

        [Description("The compression level to use when sending import streams to shards during smuggler import")]
        [DefaultValue(CompressionLevel.NoCompression)]
        [ConfigurationEntry("Sharding.Import.CompressionLevel", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public CompressionLevel CompressionLevel { get; set; }

        [Description("Accept compressed HTTP responses from shards")]
        [DefaultValue(false)]
        [ConfigurationEntry("Sharding.ShardExecutor.UseHttpDecompression", ConfigurationEntryScope.ServerWideOnly)]
        public bool ShardExecutorUseHttpDecompression { get; set; }

        [Description("Use compression when sending HTTP requests to shards")]
        [DefaultValue(false)]
        [ConfigurationEntry("Sharding.ShardExecutor.UseHttpCompression", ConfigurationEntryScope.ServerWideOnly)]
        public bool ShardExecutorUseHttpCompression { get; set; }

        [Description("Enable the timeout of the orchestrator's requests to the shards")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Sharding.OrchestratorTimeoutInMin", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting OrchestratorTimeout { get; set; }
    }
}
