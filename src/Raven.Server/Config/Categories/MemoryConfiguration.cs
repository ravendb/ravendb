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

            LowMemoryLimit = Size.Min(
                new Size(2, SizeUnit.Gigabytes), 
                memoryInfo.TotalPhysicalMemory / 10);
        }

        [Description("The minimum amount of available memory RavenDB will attempt to achieve (free memory lower than this value will trigger low memory behavior)")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Memory.LowMemoryLimitInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size LowMemoryLimit { get; set; }

        [Description("EXPERT: The minimum amount of committed memory percentage that RavenDB will attempt to ensure remains available. Reducing this value too much may cause RavenDB to fail if there is not enough memory available for the operation system to handle operations.")]
        [DefaultValue(0.05f)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Memory.MinimumFreeCommittedMemoryPercentage", ConfigurationEntryScope.ServerWideOnly)]
        public float MinimumFreeCommittedMemoryPercentage { get; set; }

        [Description("EXPERT: The maximum amount of committed memory that RavenDB will attempt to ensure remains available. Reducing this value too much may cause RavenDB to fail if there is not enough memory available for the operation system to handle operations.")]
        [DefaultValue(128)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Memory.MaxFreeCommittedMemoryToKeepInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size MaxFreeCommittedMemoryToKeepInMb { get; set; }
    }
}
