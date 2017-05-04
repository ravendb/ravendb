using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Config.Categories
{
    public class MemoryConfiguration : ConfigurationCategory
    {
        public MemoryConfiguration()
        {
            var memoryInfo = MemoryInformation.GetMemoryInfo();

            LowMemoryDetection = Size.Min(new Size(2, SizeUnit.Gigabytes), memoryInfo.TotalPhysicalMemory * PhysicalRatioForLowMemDetection);
        }

        [Description("The minimum amount of available memory RavenDB will attempt to achieve (free memory lower than this value will trigger low memory behavior)")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Raven/Memory/LowMemoryLimitInMB")]
        public Size LowMemoryDetection { get; set; }

        [Description("Physical Memory Ratio For Low Memory Detection")]
        [DefaultValue(0.10)]
        [ConfigurationEntry("Raven/Memory/PhysicalRatioForLowMemDetection")]
        public double PhysicalRatioForLowMemDetection { get; set; }
    }
}