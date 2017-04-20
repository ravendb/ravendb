using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

    public class DatabaseMember : ReplicationNode
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
        public List<DatabaseMember> Members = new List<DatabaseMember>();
        public List<DatabasePromotable> Promotables = new List<DatabasePromotable>();
        public List<DatabaseWatcher> Watchers = new List<DatabaseWatcher>();

        public class ConnectionChangeStatus
        {
            public bool Add;
            public bool Remove => !Add;
            public ReplicationNode Node;
        }

        public Dictionary<string,string> NameToUrlMap = new Dictionary<string, string>();
        public bool RelevantFor(string nodeTag)
        {
            return Members.Exists(m => m.NodeTag == nodeTag) ||
                   Promotables.Exists(p => p.NodeTag == nodeTag) ||
                   Watchers.Exists(w => w.NodeTag == nodeTag);
        }

        public IEnumerable<ReplicationNode> GetDestinations(string nodeTag, string databaseName)
        {
            var list = new List<ReplicationNode>();
            list.AddRange(Watchers.Where(w => IsItMyTask(w, nodeTag)));
            list.AddRange(Promotables.Where(p => IsItMyTask(p, nodeTag)));
            list.AddRange(Members.Where(m => m.NodeTag != nodeTag));
            list.Sort();
            return list;
        }

        public void AddMember(string nodeTag, string databaseName)
        {
            Members.Add(new DatabaseMember
            {
                NodeTag = nodeTag,
                Url = NameToUrlMap[nodeTag],
                Database = databaseName
            });
        }

        public (List<ReplicationNode> nodesToAdd, List<ReplicationNode> nodesToRemove) FindConnectionChanges(DatabaseTopology other, string nodeTag, string databaseName)
        {
            var oldDestinations = GetDestinations(nodeTag, databaseName);
            var newDestinations = other.GetDestinations(nodeTag, databaseName);

            var addDestinations = new List<ReplicationNode>();
            var removeDestinations = new List<ReplicationNode>();
            
            // this will work because the destinations are sorted. 
            using (var oldEnum = oldDestinations.GetEnumerator())
            using (var newEnum = newDestinations.GetEnumerator())
            {
                newEnum.MoveNext();
                oldEnum.MoveNext();
                while (oldEnum.Current != null && newEnum.Current != null)
                {
                    if (oldEnum.Current.CompareTo(newEnum.Current) > 0)
                    {
                        // add new
                        addDestinations.Add(newEnum.Current);
                        newEnum.MoveNext();
                        continue;
                    }
                    if (oldEnum.Current.CompareTo(newEnum.Current) < 0)
                    {
                        // remove old
                        removeDestinations.Add(oldEnum.Current);
                        oldEnum.MoveNext();
                        continue;
                    }
                    newEnum.MoveNext();
                    oldEnum.MoveNext();
                }

                // the remaining nodes of the old destinations should be removed
                while (oldEnum.Current != null)
                {
                    removeDestinations.Add(oldEnum.Current);
                    oldEnum.MoveNext();
                }

                // the remaining nodes of the new destinations should be added
                while (newEnum.Current != null)
                {
                    addDestinations.Add(newEnum.Current);
                    newEnum.MoveNext();
                }
            }
            return (addDestinations,removeDestinations);
        }
   
        public IEnumerable<string> AllNodes
        {
            get
            {
                foreach (var member in Members)
                {
                    yield return member.NodeTag;
                }
                foreach (var promotable in Promotables)
                {
                    yield return promotable.NodeTag;
                }
                foreach (var watcher in Watchers)
                {
                    yield return watcher.NodeTag;
                }
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Members)] = new DynamicJsonArray(Members.Select(m => m.ToJson())),
                [nameof(Promotables)] = new DynamicJsonArray(Promotables.Select(p => p.ToJson())),
                [nameof(Watchers)] = new DynamicJsonArray(Watchers.Select(w => w.ToJson())),
                [nameof(NameToUrlMap)] = DynamicJsonValue.Convert(NameToUrlMap)
            };
        }

        public void RemoveFromTopology(string delDbFromNode)
        {
            Promotables.RemoveAll(p=> p.NodeTag == delDbFromNode);
            Members.RemoveAll(m=> m.NodeTag == delDbFromNode);
        }

        public bool IsItMyTask(IDatabaseTask task, string nodeTag)
        {
            var myPosition = Members.FindIndex(s => s.NodeTag == nodeTag);
            if (myPosition == -1) //not a member
            {
                return false;
            }

            //TODO : ask Oren here about suspentions.. (review comments in github)
            return Hashing.JumpConsistentHash.Calculate(task.GetTaskKey(), Members.Count) == myPosition;
        }
    }
}
