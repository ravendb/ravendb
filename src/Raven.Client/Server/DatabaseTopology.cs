using System.Collections.Generic;
using Raven.Client.Documents.Replication;
using Raven.Client.Http;
using Sparrow;
using Sparrow.Json.Parsing;
using System.Linq;
using Raven.Client.Server.Operations;

namespace Raven.Client.Server
{
    public interface IDatabaseTask
    {
        ulong GetTaskKey();
    }

    public class LeaderStamp : IDynamicJson
    {
        public long Index = -1;
        public long Term = -1;
        public long LeadersTicks = -1;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Index)] = Index,
                [nameof(Term)] = Term,
                [nameof(LeadersTicks)] = LeadersTicks
            };
        }
    }

    public class PromotableTask : IDatabaseTask
    {
        private readonly string _tag;
        private readonly string _url;
        private readonly string _name;

        public PromotableTask(string tag, string url, string name)
        {
            _tag = tag;
            _url = url;
            _name = name;
        }

        protected static ulong CalculateStringHash(string s)
        {
            return string.IsNullOrEmpty(s) ? 0 : Hashing.XXHash64.Calculate(s, Encodings.Utf8);
        }

        public ulong GetTaskKey()
        {
            var hashCode = CalculateStringHash(_tag);
            hashCode = (hashCode * 397) ^ CalculateStringHash(_url);
            return (hashCode * 397) ^ CalculateStringHash(_name);
        }
    }

    public class InternalReplication : ReplicationNode
    {
        public string NodeTag;
        public override string FromString()
        {
            return $"[{NodeTag}/{Url}]";
        }
    }

    public class DatabaseTopology
    {
        public List<string> Members = new List<string>();
        public List<string> Promotables = new List<string>();
        public List<string> Rehabs = new List<string>();

        public Dictionary<string, string> DemotionReasons = new Dictionary<string, string>();
        public Dictionary<string, DatabasePromotionStatus> PromotablesStatus = new Dictionary<string, DatabasePromotionStatus>();

        public LeaderStamp Stamp;
        public bool DynamicNodesDistribution = true;
        public int ReplicationFactor = 1;

        public bool RelevantFor(string nodeTag)
        {
            return Members.Contains(nodeTag) ||
                   Rehabs.Contains(nodeTag) ||
                   Promotables.Contains(nodeTag);
        }
        
        public List<ReplicationNode> GetDestinations(string nodeTag, string databaseName, ClusterTopology clusterTopology, bool isPassive)
        {
            var list = new List<string>();
            var destinations = new List<ReplicationNode>();

            if (Members.Contains(nodeTag) == false) // if we are not a member we can't have any destinations
                return destinations;

            foreach (var member in Members)
            {
                if (member == nodeTag) //skip me
                    continue;
                list.Add(clusterTopology.GetUrlFromTag(member));
            }
            foreach (var promotable in Promotables.Concat(Rehabs))
            {
                var url = clusterTopology.GetUrlFromTag(promotable);
                if (WhoseTaskIsIt(new PromotableTask(promotable, url, databaseName), isPassive) == nodeTag)
                {
                    list.Add(url);
                }
            }
            // remove nodes that are not in the raft cluster topology
            list.RemoveAll(url => clusterTopology.TryGetNodeTagByUrl(url).hasUrl == false);

            foreach (var url in list)
            {
                destinations.Add(new InternalReplication
                {
                    NodeTag = clusterTopology.TryGetNodeTagByUrl(url).nodeTag,
                    Url = url,
                    Database = databaseName
                });
            }

            return destinations;
        }

        // Find changes in the connection of the internal database group
        public static (HashSet<string> addDestinations, HashSet<string> removeDestinations) 
            InternalReplicationChanges(List<ReplicationNode> oldDestinations, List<ReplicationNode> newDestinations)
        {
            var oldList = new List<string>();
            var newList = new List<string>();

            if (oldDestinations != null)
            {
                oldList.AddRange(oldDestinations.Select(s => s.Url));
            }
            if (newDestinations != null)
            {
                newList.AddRange(newDestinations.Select(s => s.Url));
            }

            var addDestinations = new HashSet<string>(newList);
            var removeDestinations = new HashSet<string>(oldList);

            foreach (var destination in newList)
            {
                if (removeDestinations.Contains(destination))
                {
                    removeDestinations.Remove(destination);
                    addDestinations.Remove(destination);
                }
            }

            return (addDestinations, removeDestinations);
        }

        public IEnumerable<string> AllNodes
        {
            get
            {
                foreach (var member in Members)
                {
                    yield return member;
                }
                foreach (var promotable in Promotables)
                {
                    yield return promotable;
                }
                foreach (var rehab in Rehabs)
                {
                    yield return rehab;
                }
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Members)] = new DynamicJsonArray(Members),
                [nameof(Promotables)] = new DynamicJsonArray(Promotables),
                [nameof(Rehabs)] = new DynamicJsonArray(Rehabs),
                [nameof(Stamp)] = Stamp.ToJson(),
                [nameof(PromotablesStatus)] = DynamicJsonValue.Convert(PromotablesStatus),
                [nameof(DemotionReasons)] = DynamicJsonValue.Convert(DemotionReasons),
                [nameof(DynamicNodesDistribution)] = DynamicNodesDistribution,
                [nameof(ReplicationFactor)] = ReplicationFactor
            };
        }

        public void RemoveFromTopology(string delDbFromNode)
        {
            Members.RemoveAll(m => m == delDbFromNode);
            Promotables.RemoveAll(p => p == delDbFromNode);
            Rehabs.RemoveAll(r => r == delDbFromNode);
        }

        public string WhoseTaskIsIt(IDatabaseTask task, bool inPassiveState)
        {
            if (inPassiveState)
                return null;

            var topology = new List<string>(Members);
            topology.AddRange(Promotables);
            topology.AddRange(Rehabs);
            topology.Sort();

            if (topology.Count == 0)
                return null; // this is probably being deleted now, no one is able to run tasks

            var key = task.GetTaskKey();
            while (true)
            {
                var index = (int)Hashing.JumpConsistentHash.Calculate(key, topology.Count);
                var entry = topology[index];
                if (Members.Contains(entry))
                    return entry;

                topology.RemoveAt(index);
                if (topology.Count == 0)
                    return null; // all nodes in the topology are probably in rehab


                // rehash so it will likely go to a different member in the cluster
                key = Hashing.Mix(key);
            }
        }

    }
}
