using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Config.Categories
{
    public class PerformanceHintsConfiguration : ConfigurationCategory
    {
        public PerformanceHintsConfiguration()
        {
            MinSwapSize = MemoryInformation.TotalPhysicalMemory;
        }
        [Description("The size of a document after which it will get into the huge documents collection")]
        [DefaultValue(5)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("PerformanceHints.Documents.HugeDocumentSizeInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size HugeDocumentSize { get; set; }

        [Description("The maximum size of the huge documents collection")]
        [DefaultValue(100)]
        [ConfigurationEntry("PerformanceHints.Documents.HugeDocumentsCollectionSize", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int HugeDocumentsCollectionSize { get; set; }

        [Description("The maximum amount of index outputs per document after which we send a performance hint")]
        [DefaultValue(1024)]
        [ConfigurationEntry("PerformanceHints.Indexing.MaxIndexOutputsPerDocument", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxWarnIndexOutputsPerDocument { get; set; }

        [Description("The maximum amount of results after which we will create a performance hint")]
        [DefaultValue(2048)]
        [ConfigurationEntry("PerformanceHints.MaxNumberOfResults", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxNumberOfResults { get; set; }

        [Description("Request latency threshold before the server would issue a performance hint")]
        [ConfigurationEntry("PerformanceHints.TooLongRequestThresholdInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        public TimeSetting TooLongRequestThreshold { get; set; }
        
        [Description("The minimum swap size. If the swap size is lower a notification will arise")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("PerformanceHints.Memory.MinSwapSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size MinSwapSize { get; set; }
    }
}
