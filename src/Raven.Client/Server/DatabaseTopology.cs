using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Replication;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents
{
    public class ConflictSolver
    {
        public string DatabaseResolverId;
        public Dictionary<string, ScriptResolver> ResolveByCollection;
        public bool ResolveToLatest;

        public bool ConflictResolutionChanged(ConflictSolver other)
        {
            if (ResolveToLatest != other.ResolveToLatest)
                return true;
            if (DatabaseResolverId != other.DatabaseResolverId)
                return true;
            if (ResolveByCollection == null && other.ResolveByCollection == null)
                return false;

            if (ResolveByCollection != null && other.ResolveByCollection != null)
            {
                return ResolveByCollection.SequenceEqual(other.ResolveByCollection) == false;
            }
            return true;
        }


        public bool IsEmpty()
        {
            return
                ResolveByCollection?.Count == 0 &&
                ResolveToLatest == false &&
                DatabaseResolverId == null;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DatabaseResolverId)] = DatabaseResolverId,
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

    public class ScriptResolver
    {
        public string Script { get; set; }
        public DateTime LastModifiedTime { get; } = DateTime.UtcNow;

        public object ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Script)] = Script,
                [nameof(LastModifiedTime)] = LastModifiedTime
            };
        }

        public override bool Equals(object obj)
        {
            var resolver = obj as ScriptResolver;
            if (resolver == null)
                return false;
            return string.Equals(Script, resolver.Script, StringComparison.OrdinalIgnoreCase) && LastModifiedTime == resolver.LastModifiedTime;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Script != null ? Script.GetHashCode() : 0) * 397) ^ LastModifiedTime.GetHashCode();
            }
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
        public bool RelevantFor(string nodeTag)
        {
            return Members.Contains(nodeTag) ||
                   Promotables.Contains(nodeTag) ||
                   Watchers.Exists(w => w.NodeTag == nodeTag);
        }

        public IEnumerable<ReplicationNode> GetDestinations(string nodeTag, string databaseName)
        {
            var watchers = Watchers.Where(w => IsItMyTask(w, nodeTag));

            return AllNodes.Where(n => n != nodeTag).Select(n => new ReplicationNode{
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
                [nameof(NameToUrlMap)] = DynamicJsonValue.Convert(NameToUrlMap)
            };
        }

        public void RemoveFromTopology(string delDbFromNode)
        {
            Members.Remove(delDbFromNode);
            Promotables.Remove(delDbFromNode);
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
