using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class PerformanceHintsConfiguration : ConfigurationCategory
    {
        [Description("The maximum document size after which it will get into the huge documents collection")]
        [DefaultValue(5 * 1024 * 1024)]
        [ConfigurationEntry("Raven/Databases/MaxWarnSizeHugeDocuments")]
        public int MaxWarnSizeHugeDocuments { get; set; }

        [Description("The maximum size of the huge documents collection")]
        [DefaultValue(100)]
        [ConfigurationEntry("Raven/Databases/MaxCollectionSizeHugeDocuments")]
        public int MaxCollectionSizeHugeDocuments { get; set; }

        [Description("The maximum amount of index outputs per document")]
        [DefaultValue(1024)]
        [ConfigurationEntry("Raven/Indexing/MaxWarnIndexOutputsPerDocument")]
        public int MaxWarnIndexOutputsPerDocument { get; set; }
    }
}