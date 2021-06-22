using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Raven.Client.Documents.Replication;
using Raven.Client.Http;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide
{
    public enum DatabasePromotionStatus
    {
        WaitingForFirstPromotion,
        NotResponding,
        IndexNotUpToDate,
        ChangeVectorNotMerged,
        WaitingForResponse,
        Ok,
        OutOfCpuCredits,
        EarlyOutOfMemory,
        HighDirtyMemory
    }

    public interface IDatabaseTask
    {
        ulong GetTaskKey();

        string GetMentorNode();

        string GetDefaultTaskName();

        string GetTaskName();

        bool IsResourceIntensive();
    }

    public interface IDatabaseTaskStatus
    {
        string NodeTag { get; }
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

        public string GetDefaultTaskName()
        {
            return _name;
        }

        public string GetTaskName()
        {
            return _name;
        }

        public bool IsResourceIntensive()
        {
            return false;
        }
    }

    public class InternalReplication : ReplicationNode
    {
        private string _nodeTag;

        public string NodeTag
        {
            get => _nodeTag;
            set
            {
                if (HashCodeSealed)
                    throw new InvalidOperationException(
$"NodeTag of 'InternalReplication' can't be modified after 'GetHashCode' was invoked, if you see this error it is likley a bug (NodeTag={_nodeTag} value={value} Url={Url}).");
                _nodeTag = value;
            }
        }

        public override string FromString()
        {
            return $"[{NodeTag}/{Url}]";
        }

        public override bool IsEqualTo(ReplicationNode other)
        {
            if (other is InternalReplication internalNode)
            {
                return base.IsEqualTo(internalNode) &&
                       string.Equals(Url, internalNode.Url, StringComparison.OrdinalIgnoreCase);
            }
            return false;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (int)CalculateStringHash(NodeTag);
                HashCodeSealed = true;
                return hashCode;
            }
        }
    }

    internal static class ThreadSafeRandom
    {
        [ThreadStatic]
        private static Random _random;

        // Fisher-Yates shuffle algorithm
        public static void Shuffle<T>(this IList<T> list)
        {
            var n = list.Count;
            while (n > 1)
            {
                n--;
                var k = (_random ??= new Random()).Next(n + 1);
                var value = list[k];
                list[k] = list[n];
                list[n] = value;
            }
        }
    }

    public class DatabaseTopology
    {
        public List<string> Members = new List<string>();
        public List<string> Promotables = new List<string>();
        public List<string> Rehabs = new List<string>();

        public Dictionary<string, string> PredefinedMentors = new Dictionary<string, string>();
        public Dictionary<string, string> DemotionReasons = new Dictionary<string, string>();
        public Dictionary<string, DatabasePromotionStatus> PromotablesStatus = new Dictionary<string, DatabasePromotionStatus>();

        public LeaderStamp Stamp;
        public bool DynamicNodesDistribution;
        public int ReplicationFactor = 1;
        public List<string> PriorityOrder;

        internal void ReorderMembers()
        {
            Members.Shuffle();

            if (PriorityOrder == null || PriorityOrder.Count == 0)
                return;

            var members = new List<string>();
            for (int i = 0; i < PriorityOrder.Count; i++)
            {
                var priorityNode = PriorityOrder[i];
                if (Members.Contains(priorityNode))
                    members.Add(priorityNode);
            }

            for (int i = 0; i < Members.Count; i++)
            {
                var member = Members[i];
                if (members.Contains(member) == false)
                    members.Add(member);
            }

            Members = members;
        }

        internal bool TryUpdateByPriorityOrder()
        {
            if (IsReorderNeeded() == false)
                return false;

            var originalOrder = new List<string>(Members);
            ReorderMembers();

            if (originalOrder.SequenceEqual(Members))
                return false; // members hasn't changed after the reorder

            return true;
        }

        private bool IsReorderNeeded()
        {
            if (PriorityOrder == null || PriorityOrder.Count == 0)
                return false;

            for (var index = 0; index < Math.Min(Members.Count, PriorityOrder.Count); index++)
            {
                var member = Members[index];
                if (PriorityOrder[index] == member)
                    return true;
            }

            return false;
        }

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

            if (Promotables.Contains(myTag)) // if we are a promotable we can't have any destinations
                return destinations;

            var nodes = Members.Concat(Rehabs);

            foreach (var node in nodes)
            {
                if (node == myTag) // skip me
                    continue;
                if (deletionInProgress != null && deletionInProgress.ContainsKey(node))
                    continue;
                list.Add(clusterTopology.GetUrlFromTag(node));
            }

            foreach (var promotable in Promotables)
            {
                if (deletionInProgress != null && deletionInProgress.ContainsKey(promotable))
                    continue;

                var url = clusterTopology.GetUrlFromTag(promotable);
                PredefinedMentors.TryGetValue(promotable, out var mentor);
                if (WhoseTaskIsIt(state, new PromotableTask(promotable, url, databaseName, mentor), null) == myTag)
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

        public bool EntireDatabasePendingDeletion(Dictionary<string, DeletionInProgressStatus> deletionInProgress)
        {
            if (Count == 0)
                return true;

            if (deletionInProgress?.Count > 0)
            {
                foreach (var node in AllNodes)
                {
                    if (deletionInProgress.ContainsKey(node) == false)
                        return false;
                }
                return true;
            }

            return false;
        }

        public static (List<string> Members, List<string> Promotables, List<string> Rehabs) Reorder(DatabaseTopology topology, List<string> order)
        {
            if (topology.Count != order.Count
                || topology.AllNodes.All(order.Contains) == false)
            {
                throw new ArgumentException("The reordered list doesn't correspond to the existing nodes of the database group.");
            }

            var newMembers = new List<string>();
            var newPromotables = new List<string>();
            var newRehabs = new List<string>();

            foreach (var node in order)
            {
                if (topology.Members.Contains(node))
                {
                    newMembers.Add(node);
                }
                else if (topology.Promotables.Contains(node))
                {
                    newPromotables.Add(node);
                }
                else if (topology.Rehabs.Contains(node))
                {
                    newRehabs.Add(node);
                }
                else
                {
                    throw new ArgumentException($"Can't find node {node} in the topology");
                }
            }

            return (newMembers, newPromotables, newRehabs);
        }

        // Find changes in the connection of the internal database group
        internal static (HashSet<string> AddedDestinations, HashSet<string> RemovedDestiantions) FindChanges(IEnumerable<ReplicationNode> oldDestinations, List<ReplicationNode> newDestinations)
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

        public string DatabaseTopologyIdBase64;
        public string ClusterTransactionIdBase64;

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
                [nameof(Stamp)] = Stamp?.ToJson(),
                [nameof(PromotablesStatus)] = DynamicJsonValue.Convert(PromotablesStatus),
                [nameof(DemotionReasons)] = DynamicJsonValue.Convert(DemotionReasons),
                [nameof(DynamicNodesDistribution)] = DynamicNodesDistribution,
                [nameof(ReplicationFactor)] = ReplicationFactor,
                [nameof(DatabaseTopologyIdBase64)] = DatabaseTopologyIdBase64,
                [nameof(ClusterTransactionIdBase64)] = ClusterTransactionIdBase64,
                [nameof(PriorityOrder)] = PriorityOrder != null ? new DynamicJsonArray(PriorityOrder) : null
            };
        }

        public void RemoveFromTopology(string delDbFromNode)
        {
            Members.RemoveAll(m => m == delDbFromNode);
            Promotables.RemoveAll(p => p == delDbFromNode);
            Rehabs.RemoveAll(r => r == delDbFromNode);

            DemotionReasons.Remove(delDbFromNode);
            PromotablesStatus.Remove(delDbFromNode);
            PredefinedMentors.Remove(delDbFromNode);
        }

        public string WhoseTaskIsIt(
            RachisState state,
            IDatabaseTask task,
            Func<string> getLastResponsibleNode)
        {
            if (state == RachisState.Candidate || state == RachisState.Passive)
                return null;

            var mentorNode = task.GetMentorNode();
            if (mentorNode != null)
            {
                if (Members.Contains(mentorNode))
                    return mentorNode;
            }

            var lastResponsibleNode = getLastResponsibleNode?.Invoke();
            if (lastResponsibleNode != null)
                return lastResponsibleNode;

            var topology = new List<string>(Members);
            topology.AddRange(Promotables);
            topology.AddRange(Rehabs);
            topology.Sort();

            if (task.IsResourceIntensive() && Members.Count > 1)
            {
                // if resource intensive operation, we don't want to have it on the first node of the database topology
                return FindNodeForIntensiveOperation(task.GetTaskKey(), topology);
            }

            return FindNode(task.GetTaskKey(), topology);
        }

        private string FindNodeForIntensiveOperation(ulong key, List<string> topology)
        {
            var firstNode = Members[0];
            topology.Remove(firstNode);

            var node = FindNode(key, topology);
            if (node == null)
                return firstNode;

            return node;
        }

        private string FindNode(ulong key, List<string> topology)
        {
            if (topology.Count == 0)
                return null; // this is probably being deleted now, no one is able to run tasks

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
