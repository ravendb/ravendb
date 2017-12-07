using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Raven.Client.Documents.Replication;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide
{
    public interface IDatabaseTask
    {
        ulong GetTaskKey();
        string GetMentorNode();
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
        private readonly string _mentorNode;

        public PromotableTask(string tag, string url, string name, string mentorNode = null)
        {
            _tag = tag;
            _url = url;
            _name = name;
            _mentorNode = mentorNode;
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

        public string GetMentorNode()
        {
            return _mentorNode;
        }
    }

    public class InternalReplication : ReplicationNode
    {
        public string NodeTag;
        public override string FromString()
        {
            return $"[{NodeTag}/{Url}]";
        }

        public override bool IsEqualTo(ReplicationNode other)
        {
            return base.IsEqualTo(other) && 
                   string.Equals(Url, other.Url, StringComparison.OrdinalIgnoreCase);
        }
        
        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (int)CalculateStringHash(Url);
                return hashCode;
            }
        }
    }

    public class DatabaseTopology
    {
        public List<string> Members = new List<string>();
        public List<string> Promotables = new List<string>();
        public List<string> Rehabs = new List<string>();

        public Dictionary<string,string> PredefinedMentors = new Dictionary<string, string>();
        public Dictionary<string, string> DemotionReasons = new Dictionary<string, string>();
        public Dictionary<string, DatabasePromotionStatus> PromotablesStatus = new Dictionary<string, DatabasePromotionStatus>();

        public LeaderStamp Stamp;
        public bool DynamicNodesDistribution;
        public int ReplicationFactor = 1;

        public bool RelevantFor(string nodeTag)
        {
            return Members.Contains(nodeTag) ||
                   Rehabs.Contains(nodeTag) ||
                   Promotables.Contains(nodeTag);
        }

        public List<ReplicationNode> GetDestinations(string myTag, string databaseName, Dictionary<string, DeletionInProgressStatus> deletionInProgress,
            ClusterTopology clusterTopology, RachisState state)
        {
            var list = new List<string>();
            var destinations = new List<ReplicationNode>();
            
            if (Rehabs.Contains(myTag))
            {
                // if we are in rehab, but now are back online. We need to send all the new documents that we might have.
                var url = clusterTopology.GetUrlFromTag(myTag);
                var mentor = WhoseTaskIsIt(new PromotableTask(myTag, url, databaseName), state);
                if (mentor != null)
                {
                    destinations.Add(new InternalReplication
                    {
                        NodeTag = mentor,
                        Url = clusterTopology.GetUrlFromTag(mentor),
                        Database = databaseName
                    });
                }
                return destinations;
            }
            
            if (Members.Contains(myTag) == false) // if we are not a member we can't have any destinations
                return destinations;

            foreach (var member in Members)
            {
                if (member == myTag) //skip me
                    continue;
                if (deletionInProgress != null && deletionInProgress.ContainsKey(member))
                    continue;
                list.Add(clusterTopology.GetUrlFromTag(member));
            }
            foreach (var promotable in Promotables.Concat(Rehabs))
            {
                if (deletionInProgress != null && deletionInProgress.ContainsKey(promotable))
                    continue;
                
                var url = clusterTopology.GetUrlFromTag(promotable);
                PredefinedMentors.TryGetValue(promotable, out var mentor);
                if (WhoseTaskIsIt(new PromotableTask(promotable, url, databaseName, mentor), state) == myTag)
                {
                    list.Add(url);
                }
            }
            // remove nodes that are not in the raft cluster topology
            list.RemoveAll(url => clusterTopology.TryGetNodeTagByUrl(url).HasUrl == false);

            foreach (var url in list)
            {
                destinations.Add(new InternalReplication
                {
                    NodeTag = clusterTopology.TryGetNodeTagByUrl(url).NodeTag,
                    Url = url,
                    Database = databaseName
                });
            }
            
            return destinations;
        }

        // Find changes in the connection of the internal database group
        internal static (HashSet<string> AddedDestinations, HashSet<string> RemovedDestiantions) FindChanges(List<ReplicationNode> oldDestinations, List<ReplicationNode> newDestinations)
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

        [JsonIgnore]
        public int Count => Members.Count + Promotables.Count + Rehabs.Count;

        [JsonIgnore]
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

        public string WhoseTaskIsIt(IDatabaseTask task, RachisState state)
        {
            if (state == RachisState.Candidate || state == RachisState.Passive)
                return null;

            var mentorNode = task.GetMentorNode();
            if (mentorNode != null)
            {
                if (Members.Contains(mentorNode))
                    return mentorNode;
            }

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
