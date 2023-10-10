using System.ComponentModel;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.ExportImport)]
    public class ExportImportConfiguration : ConfigurationCategory
    {
        [Description("Compression algorithm that is used to perform exports.")]
        [DefaultValue(ExportCompressionAlgorithm.Zstd)]
        [ConfigurationEntry("Export.Compression.Algorithm", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public ExportCompressionAlgorithm CompressionAlgorithm { get; set; }
    }
}
