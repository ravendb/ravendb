using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Replication;
using Sparrow;
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
            var isResolveByCollectionNull = ResolveByCollection == null;
            var isOtherResolveByCollectionNull = other.ResolveByCollection == null;

            if (isResolveByCollectionNull || isOtherResolveByCollectionNull)
                return true;

            return ResolveToLatest == other.ResolveToLatest && (bool)ResolveByCollection?.SequenceEqual(other.ResolveByCollection);
        }

        public bool IsEmpty()
        {
            return ResolveByCollection?.Count == 0 || (ResolveToLatest == false && DatabaseResovlerId == null);
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DatabaseResovlerId)] = DatabaseResovlerId,
                [nameof(ResolveToLatest)] = ResolveToLatest,
                [nameof(ResolveByCollection)] = new DynamicJsonArray
                {
                    ResolveByCollection != null ? 
                        ResolveByCollection.Select( item => new DynamicJsonValue
                        {
                            [nameof(item.Key)] = item.Value.ToJson()
                        }) :
                        new DynamicJsonValue[0]
                }
            };
        }
    }

    public interface IDatabaseTask
    {
        ulong GetTaskKey();
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
        //TODO : not sure what this should do (any even why it is needed)
        //TODO : leaving this for now, perhaps this is here by design
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
            if (myPosition == -1) //not a member
            {
                return false;
            }

            //TODO : ask Oren here about suspentions.. (review comments in github)
            return Hashing.JumpConsistentHash.Calculate(task.GetTaskKey(), Members.Count) == myPosition;
        }
    }
}
