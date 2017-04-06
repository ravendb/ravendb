using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Replication;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents
{
    public class ConflictSolver
    {
        public string DatabaseResovlerId;
        public Dictionary<string, ScriptResolver> ResolveByCollection;
        public bool ResolveToLatest;

        public bool ConflictResolutionChanged(ConflictSolver other)
        {
            if ((ResolveByCollection == null ^ other.ResolveByCollection == null) == false)
                return true;

            return ResolveToLatest == other.ResolveToLatest &&
                   (ResolveByCollection?.SequenceEqual(other.ResolveByCollection) ?? true);
        }

        public bool IsEmpty()
        {
            return ResolveToLatest == false && DatabaseResovlerId == null && ResolveByCollection?.Count == 0;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DatabaseResovlerId)] = DatabaseResovlerId,
                [nameof(ResolveToLatest)] = ResolveToLatest,
                [nameof(ResolveByCollection)] = new DynamicJsonArray
                {
                    ResolveByCollection.Select( item => new DynamicJsonValue
                    {
                        [nameof(item.Key)] = item.Value.ToJson()
                    })
                }
            };
        }
    }

    public interface IDatabaseTask
    {        
    }

    public class DatabaseWatcher : ReplicationNode, IDatabaseTask
    {
    }

    public class DatabasePromotable : ReplicationNode, IDatabaseTask
    {
    }

    public class DatabaseTopology
    {
        public List<string> Members = new List<string>();
        public List<string> Promotables = new List<string>();
        public List<DatabaseWatcher> Watchers = new List<DatabaseWatcher>();

        public Dictionary<string,string> NameToUrlMap = new Dictionary<string, string>();

        // If we want to prevent from a replication we set an entry 
        // e.g. setting ("A","C") will prevent replication from server A to C for this database.
        public Dictionary<string,string> CustomConnectionBlocker = new Dictionary<string, string>();

        public bool RelevantFor(string nodeTag)
        {
            return Members.Contains(nodeTag) ||
                   Promotables.Contains(nodeTag) ||
                   Watchers.Exists(w => w.NodeTag == nodeTag);
        }

        public IEnumerable<ReplicationNode> GetDestinations(string nodeTag, string databaseName)
        {
            var except = CustomConnectionBlocker
                .Where(item => item.Key == nodeTag)
                .Select(item => item.Key)
                .Concat(new []{nodeTag});

            var watchers = Watchers.Where(w => IsItMyTask(w, nodeTag));

            return AllNodes.Except(except).Select(n => new ReplicationNode{
                NodeTag = n,
                Url = NameToUrlMap[n],
                Database = databaseName
            }).Concat(watchers);
        }

        public bool MyConnectionChanged(DatabaseTopology other, string nodeTag, string databaseName)
        {
            return GetDestinations(nodeTag,databaseName).SequenceEqual(other.GetDestinations(nodeTag, databaseName)) == false;
        }

        public IEnumerable<string> AllNodes
        {
            get
            {
                foreach (var member in Members)
                {
                    yield return member;
                }
                foreach (var member in Promotables)
                {
                    yield return member;
                }
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Members)] = new DynamicJsonArray(Members),
                [nameof(Promotables)] = new DynamicJsonArray(Promotables),
                [nameof(Watchers)] = new DynamicJsonArray(Watchers.Select(w => w.ToJson())),
                [nameof(CustomConnectionBlocker)] = DynamicJsonValue.Convert(CustomConnectionBlocker),
                [nameof(NameToUrlMap)] = DynamicJsonValue.Convert(NameToUrlMap)
            };
        }

        public void RemoveFromTopology(string delDbFromNode)
        {
            Members.Remove(delDbFromNode);
            Promotables.Remove(delDbFromNode);
            CustomConnectionBlocker.Remove(delDbFromNode);
        }

        public bool IsItMyTask(IDatabaseTask task, string nodeTag)
        {
            var myPosition = Members.FindIndex(s => s == nodeTag);
            return JumpConsistentHash((ulong)task.GetHashCode(), Members.Count) == myPosition;
        }

        public static long JumpConsistentHash(ulong key, int numBuckets)
        {
            long b = 1L;
            long j = 0;
            while (j < numBuckets)
            {
                b = j;
                key = key * 2862933555777941757UL + 1;
                j = (long)((b + 1) * ((1L << 31) / ((double)(key >> 33) + 1)));
            }
            return b;
        }
    }
}
