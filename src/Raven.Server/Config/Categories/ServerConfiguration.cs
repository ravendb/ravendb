using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public class ServerConfiguration : ConfigurationCategory
    {
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Server.MaxTimeForTaskToWaitForDatabaseToLoadInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting MaxTimeForTaskToWaitForDatabaseToLoad { get; set; }

        [Description("The server name")]
        [DefaultValue(null)]
        [ConfigurationEntry("Server.Name", ConfigurationEntryScope.ServerWideOnly)]
        public string Name { get; set; }

        [Description("EXPERT: The process affinity mask")]
        [DefaultValue(null)]
        [ConfigurationEntry("Server.ProcessAffinityMask", ConfigurationEntryScope.ServerWideOnly)]
        public long? ProcessAffinityMask { get; set; }

        [Description("EXPERT: The affinity mask to be used for indexing. Overrides the Server.NumberOfUnusedCoresByIndexes value. Should only be used if you also set Server.ProcessAffinityMask.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Server.IndexingAffinityMask", ConfigurationEntryScope.ServerWideOnly)]
        public long? IndexingAffinityMask { get; set; }

        [Description("EXPERT: The numbers of cores that will be NOT run indexing. Defaults to 1 core that is kept for all other tasks and will not run indexing.")]
        [DefaultValue(1)]
        [ConfigurationEntry("Server.NumberOfUnusedCoresByIndexes", ConfigurationEntryScope.ServerWideOnly)]
        public int NumberOfUnusedCoresByIndexes { get; set; }

        [Description("EXPERT: To let RavenDB manage burstable instance performance by scaling down background operations")]
        [DefaultValue(null)]
        [ConfigurationEntry("Server.CpuCredits.Base", ConfigurationEntryScope.ServerWideOnly)]
        public double? CpuCreditsBase { get; set; }

        [Description("EXPERT: To let RavenDB manage burstable instance performance by scaling down background operations")]
        [DefaultValue(null)]
        [ConfigurationEntry("Server.CpuCredits.Max", ConfigurationEntryScope.ServerWideOnly)]
        public double? CpuCreditsMax { get; set; }

        [Description("EXPERT: When CPU credits are exhausted to the threshold, start stopping background tasks.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Server.CpuCredits.ExhaustionBackgroundTasksThreshold", ConfigurationEntryScope.ServerWideOnly)]
        public double? CpuCreditsExhaustionBackgroundTasksThreshold { get; set; }

        [Description("EXPERT: When CPU credits are exhausted to the threshold, start rejecting requests to databases.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Server.CpuCredits.ExhaustionFailoverThreshold", ConfigurationEntryScope.ServerWideOnly)]
        public double? CpuCreditsExhaustionFailoverThreshold { get; set; }

        [Description("EXPERT: When CPU credits are exhausted backups are canceled. This value indicates after how many minutes the backup task will re-try.")]
        [DefaultValue(10)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Server.CpuCredits.ExhaustionBackupDelayInMin", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting CpuCreditsExhaustionBackupDelay { get; set; }
    }
}
