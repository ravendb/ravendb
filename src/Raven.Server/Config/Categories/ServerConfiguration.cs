using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow.Platform;
using Sparrow.Utils;

namespace Raven.Server.Config.Categories
{
    public class ServerConfiguration : ConfigurationCategory
    {
        public ServerConfiguration()
        {
            ThreadPoolMinThreads = (PlatformDetails.Is32Bits ? 2 : 4) * ProcessorInfo.ProcessorCount;
        }

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

        [Description("EXPERT: A command or executable that will provide RavenDB with the current CPU credits balance.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Server.CpuCredits.Exec", ConfigurationEntryScope.ServerWideOnly)]
        public string CpuCreditsExec { get; set; }

        [Description("EXPERT: The command line arguments for the Server.CpuCredits.Exec command or executable.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Server.CpuCredits.Exec.Arguments", ConfigurationEntryScope.ServerWideOnly)]
        public string CpuCreditsExecArguments { get; set; }

        [Description("EXPERT: The number of minutes between every invocation of the CPU Credits executable. Default: 30 minutes.")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Server.CpuCredits.Exec.SyncIntervalInMin", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting CpuCreditsExecSyncInterval { get; set; }

        [Description("EXPERT: The number of seconds to wait for the CPU Credits executable to exit. Default: 30 seconds.")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Server.CpuCredits.Exec.TimeoutInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting CpuCreditsExecTimeout { get; set; }

        [Description("EXPERT: Sets the minimum number of threads the thread pool creates on demand, as new requests are made, before switching to an algorithm for managing thread creation and destruction.")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [ConfigurationEntry("Server.ThreadPool.MinThreads", ConfigurationEntryScope.ServerWideOnly)]
        public int? ThreadPoolMinThreads { get; set; }

        [Description("EXPERT: Sets the number of requests to the thread pool that can be active concurrently. All requests above that number remain queued until thread pool threads become available.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Server.ThreadPool.MaxThreads", ConfigurationEntryScope.ServerWideOnly)]
        public int? ThreadPoolMaxThreads { get; set; }
    }
}
