using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Rachis
{
    public class ClusterTopology
    {
        public ClusterTopology(string topologyId, string apiKey, Dictionary<string, string> voters, Dictionary<string, string> promotables, Dictionary<string, string> nonVotingMembers, string lastNodeId)
        {
            TopologyId = topologyId;
            ApiKey = apiKey;
            Voters = voters;
            Promotables = promotables;
            NonVotingMembers = nonVotingMembers;
            LastNodeId = lastNodeId;
        }

        public bool Contains(string node)
        {
            return Voters.ContainsKey(node) || Promotables.ContainsKey(node) || NonVotingMembers.ContainsKey(node);
        }

        public ClusterTopology()
        {
            
        }


        public readonly string LastNodeId;
        public readonly string TopologyId;
        public readonly string ApiKey;
        public readonly Dictionary<string,string> Voters;
        public readonly Dictionary<string,string> Promotables;
        public readonly Dictionary<string,string> NonVotingMembers;
    }
}