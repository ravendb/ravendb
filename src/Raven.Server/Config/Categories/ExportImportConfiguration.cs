using System;
using System.ComponentModel;
using System.IO.Compression;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.ExportImport)]
    public class ExportImportConfiguration : ConfigurationCategory
    {
        public ExportImportConfiguration()
        {
            ZstdCompressionWorkers = BackupConfiguration.GetDefaultZstdCompressionWorkers(Environment.ProcessorCount);
        }

        [Description("Compression algorithm that is used to perform exports.")]
        [DefaultValue(ExportCompressionAlgorithm.Zstd)]
        [ConfigurationEntry("Export.Compression.Algorithm", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public ExportCompressionAlgorithm CompressionAlgorithm { get; set; }

        [Description("Compression level that is used to perform exports.")]
        [DefaultValue(CompressionLevel.Fastest)]
        [ConfigurationEntry("Export.Compression.Level", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public CompressionLevel CompressionLevel { get; set; }

        [Description("Number of worker threads compressing an export with Zstd, in parallel with reading the data. 0 compresses on the export thread. Each worker needs its own compression context, so memory grows with the number of workers and the compression level. Default: 0 below 8 cores, 2 for 8-16 cores, 4 above.")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [MinValue(0)]
        [ConfigurationEntry("Export.Compression.Zstd.Workers", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int ZstdCompressionWorkers { get; set; }
    }
}
