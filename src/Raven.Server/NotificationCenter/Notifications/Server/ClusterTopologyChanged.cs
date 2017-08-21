using System.Collections.Generic;
using Raven.Client.Http;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Server
{
    public class ClusterTopologyChanged : Notification
    {
        private ClusterTopologyChanged() : base(NotificationType.ClusterTopologyChanged)
        {
        }

        public override string Id => $"{Type}";

        public ClusterTopology Topology { get; private set; }

        public Dictionary<string, NodeStatus> Status { get; private set; }

        public string Leader { get; private set; }

        public string NodeTag { get; private set; }
        
        public long CurrentTerm { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(Topology)] = Topology.ToJson();
            json[nameof(Leader)] = Leader;
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(Status)] = DynamicJsonValue.Convert(Status);
            json[nameof(CurrentTerm)] = CurrentTerm;

            return json;
        }

        public static ClusterTopologyChanged Create(ClusterTopology clusterTopology, string leaderTag, string nodeTag, long term, Dictionary<string, NodeStatus> status)
        {
            return new ClusterTopologyChanged
            {
                Topology = clusterTopology,
                Leader = leaderTag,
                NodeTag = nodeTag,
                Status = status,
                CurrentTerm = term
            };
        }
    }
}
