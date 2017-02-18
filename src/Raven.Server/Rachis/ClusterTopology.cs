using System;

namespace Raven.Server.Rachis
{
    public class ClusterTopology
    {
        public ClusterTopology(string topologyId, string apiKey, string[] voters, string[] promotables, string[] nonVotingMembers)
        {
            TopologyId = topologyId;
            ApiKey = apiKey;
            Voters = voters;
            Promotables = promotables;
            NonVotingMembers = nonVotingMembers;
        }

        public readonly string TopologyId;
        public readonly string ApiKey;
        public readonly string[] Voters;
        public readonly string[] Promotables;
        public readonly string[] NonVotingMembers;
    }
}