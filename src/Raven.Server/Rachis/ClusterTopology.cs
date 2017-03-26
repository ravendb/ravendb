using System.Collections.Generic;

namespace Raven.Server.Rachis
{
    public class ClusterTopology
    {
        public ClusterTopology(string topologyId, string apiKey, Dictionary<string, string> members, Dictionary<string, string> promotables, Dictionary<string, string> watchers, string lastNodeId)
        {
            TopologyId = topologyId;
            ApiKey = apiKey;
            Members = members;
            Promotables = promotables;
            Watchers = watchers;
            LastNodeId = lastNodeId;
        }

        public bool Contains(string node)
        {
            return Members.ContainsKey(node) || Promotables.ContainsKey(node) || Watchers.ContainsKey(node);
        }



        public ClusterTopology()
        {
            
        }

        public string GetUrlFormTag(string tag)
        {
            string url;
            if (Members.TryGetValue(tag, out url))
            {
                return url;
            }
            if (Promotables.TryGetValue(tag, out url))
            {
                return url;
            }
            if (Watchers.TryGetValue(tag, out url))
            {
                return url;
            }
            return null;
        }

        public readonly string LastNodeId;
        public readonly string TopologyId;
        public readonly string ApiKey;
        public readonly Dictionary<string,string> Members;
        public readonly Dictionary<string,string> Promotables;
        public readonly Dictionary<string,string> Watchers;
    }
}