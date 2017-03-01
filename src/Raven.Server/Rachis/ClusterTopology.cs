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

        public ClusterTopology()
        {
            
        }

        public string TopologyId;
        public string ApiKey;
        public string[] Voters;
        public string[] Promotables;
        public string[] NonVotingMembers;
    }
}