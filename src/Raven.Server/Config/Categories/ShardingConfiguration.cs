using System.ComponentModel;
using System.IO.Compression;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Sharding)]
    public class ShardingConfiguration : ConfigurationCategory
    {
        [Description("The compression level to use when sending import streams to shards during smuggler import")]
        [DefaultValue(CompressionLevel.NoCompression)]
        [ConfigurationEntry("Sharding.Import.CompressionLevel", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public CompressionLevel CompressionLevel { get; set; }
    }
}
