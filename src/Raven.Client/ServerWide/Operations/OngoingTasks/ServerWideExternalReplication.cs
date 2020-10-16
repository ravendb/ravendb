using System;
using Raven.Client.Documents.Operations.Replication;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.OngoingTasks
{
    public class ServerWideExternalReplication : IExternalReplication, IServerWideTask
    {
        internal static string NamePrefix = "Server Wide External Replication";

        internal static string RavenConnectionStringPrefix = "Server Wide Raven Connection String";

        public bool Disabled { get; set; }

        public long TaskId { get; set; }

        public string Name { get; set; }

        public string MentorNode { get; set; }

        public TimeSpan DelayReplicationFor { get; set; }

        public string[] TopologyDiscoveryUrls { get; set; }

        public string[] ExcludedDatabases { get; set; }

        public string GetDefaultTaskName()
        {
            return $"External Replication to {string.Join(",", TopologyDiscoveryUrls)}";
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(TaskId)] = TaskId,
                [nameof(Name)] = Name,
                [nameof(MentorNode)] = MentorNode,
                [nameof(DelayReplicationFor)] = DelayReplicationFor,
                [nameof(TopologyDiscoveryUrls)] = TopologyDiscoveryUrls,
                [nameof(ExcludedDatabases)] = ExcludedDatabases
            };
        }
    }
}
