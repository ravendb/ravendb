using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Raven.NewClient.Client.Http
{
    public class TopologyNode
    {

        public List<TopologyNode> Outgoing;

        public ServerNode Node;
        
        /// <summary>
        /// If not null then only docs from specified collections are replicated and transformed / filtered according to an optional script.
        /// </summary>
        public Dictionary<string, string> SpecifiedCollections;

        /// <summary>
        /// Gets or sets if the replication will ignore this destination in the client
        /// </summary>
        public bool IgnoredClient;

        /// <summary>
        /// Gets or sets if replication to this destination is disabled in both client and server.
        /// </summary>
        public bool Disabled;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CanBeFailover() => IgnoredClient == false && Disabled == false && (SpecifiedCollections == null || SpecifiedCollections.Count == 0);

        //if true, all outgoing connections to this node are disabled
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsDisconnectedNode() => CanBeFailover() == false || Disabled || IgnoredClient || SpecifiedCollections?.Count > 0;

    }

    public class Topology
    {
        public long Etag;
        public List<TopologyNode> Outgoing;
        public ServerNode LeaderNode;
        public ReadBehavior ReadBehavior;
        public WriteBehavior WriteBehavior;
        public TopologySla SLA;
    }
}