using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public class ServerConfiguration : ConfigurationCategory
    {
        [Description("Receive timeout for all TCP connections")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Server.Tcp.ReceiveTimeoutInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting ReceiveTimeout { get; set; }

        [Description("Sending timeout for all TCP connections")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Server.Tcp.SendTimeoutInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting SendTimeout { get; set; }

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
    }
}
