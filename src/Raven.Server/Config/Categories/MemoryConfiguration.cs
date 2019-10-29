using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Sparrow;
using Sparrow.LowMemory;
using Sparrow.Platform;

namespace Raven.Server.Config.Categories
{
    public class MemoryConfiguration : ConfigurationCategory
    {
        public MemoryConfiguration()
        {
            LowMemoryLimit = Size.Min(
                new Size(2, SizeUnit.Gigabytes),
                MemoryInformation.TotalPhysicalMemory / 10);

            UseTotalDirtyMemInsteadOfMemUsage = PlatformDetails.RunningOnDocker;
        }

        [Description("The minimum amount of available memory RavenDB will attempt to achieve (free memory lower than this value will trigger low memory behavior)")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Memory.LowMemoryLimitInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size LowMemoryLimit { get; set; }

        [Description("The minimum amount of available commited memory RavenDB will attempt to achieve (free commited memory lower than this value will trigger low memory behavior)")]
        [DefaultValue(512)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Memory.LowMemoryCommitLimitInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size LowMemoryCommitLimitInMb { get; set; }

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

        [Description("EXPERT: Use entire process dirty memory instead of 'memory.usage_in_bytes minus Shared Clean Memory' value to determine machine memory usage. Applicable only when running on Linux. Default: 'true' when 'RAVEN_IN_DOCKER' environment variable is set to 'true', 'false' otherwise.")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [ConfigurationEntry("Memory.UseTotalDirtyMemInsteadOfMemUsage", ConfigurationEntryScope.ServerWideOnly)]
        public bool UseTotalDirtyMemInsteadOfMemUsage { get; set; }
    }
}
