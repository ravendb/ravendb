using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Replication;
using Raven.Client.Extensions;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Server
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
            DynamicJsonValue resolveByCollection = null;
            if(ResolveByCollection != null)
            {
                resolveByCollection = new DynamicJsonValue();
                foreach (var scriptResolver in ResolveByCollection)
                {
                    resolveByCollection[scriptResolver.Key] = scriptResolver.Value.ToJson();
                }
            }
            return new DynamicJsonValue
            {
                [nameof(DatabaseResolverId)] = DatabaseResolverId,
                [nameof(ResolveToLatest)] = ResolveToLatest,
                [nameof(ResolveByCollection)] = resolveByCollection
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

    public class DatabaseTopologyNode : ReplicationNode, IDatabaseTask
    {
    }

    public class DatabaseWatcher : ReplicationNode, IDatabaseTask, IDynamicJsonValueConvertible
    {
        public string ApiKey;
        public string CurrentTaskId;
        //TODO: change to ulong after grisha's fix is merged


        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(ApiKey)] = ApiKey;
            json[nameof(CurrentTaskId)] = CurrentTaskId;
            return json;
        }

        public override ulong GetTaskKey()
        {
            var hashCode = CalculateStringHash(Database);
            hashCode = (hashCode * 397) ^ CalculateStringHash(Url);
            return hashCode;
        }
    }
    
    public class DatabaseTopology
    {
        public List<DatabaseTopologyNode> Members = new List<DatabaseTopologyNode>(); // Member of the master to master replication inside cluster
        public List<DatabaseTopologyNode> Promotables = new List<DatabaseTopologyNode>(); // Promotable is in a receive state until Leader decides it can become a Member
        public Dictionary<string, DatabaseWatcher> Watchers = new Dictionary<string, DatabaseWatcher>(); // Watcher only receives (slave)
        //TODO: change from string to ulong after grisha's fix is merged

        public bool RelevantFor(string nodeTag)
        {
            return Members.Exists(m => m.NodeTag == nodeTag) ||
                   Promotables.Exists(p => p.NodeTag == nodeTag);
        }

        public List<ReplicationNode> GetDestinations(string nodeTag, string databaseName)
        {
            var list = new List<ReplicationNode>();
            foreach (var member in Members)
            {
                if (member.NodeTag == nodeTag) //skip me
                    continue;
                list.Add(member);
            }
            foreach (var promotable in Promotables)
            {
                if (WhoseTaskIsIt(promotable) == nodeTag)
                    list.Add(promotable);
            }
            foreach (var watcher in Watchers.Values)
            {
                if (WhoseTaskIsIt(watcher) == nodeTag)
                    list.Add(watcher);
            }
            return list;
        }

        public static (List<ReplicationNode> addDestinations, List<ReplicationNode> removeDestinations) FindConnectionChanges(List<ReplicationNode> oldDestinations, List<ReplicationNode> newDestinations)
        {
            var addDestinations = new List<ReplicationNode>();
            var removeDestinations = new List<ReplicationNode>();

            if (oldDestinations == null)
            {
                oldDestinations = new List<ReplicationNode>();
            }
            if (newDestinations == null)
            {
                newDestinations = new List<ReplicationNode>();
            }

            // this will work because the destinations are sorted. 
            using (var oldEnum = oldDestinations.GetEnumerator())
            using (var newEnum = newDestinations.GetEnumerator())
            {
                var hasNewValues = newEnum.MoveNext();
                var hasOldValues = oldEnum.MoveNext();

                while (hasNewValues && hasOldValues)
                {
                    var res = oldEnum.Current.CompareTo(newEnum.Current);
                    if (res > 0)
                    {
                        addDestinations.Add(newEnum.Current);
                        hasNewValues = newEnum.MoveNext();
                        continue;
                    }
                    if (res < 0)
                    {
                        removeDestinations.Add(oldEnum.Current);
                        hasOldValues = oldEnum.MoveNext();
                        continue;
                    }

                    hasNewValues = newEnum.MoveNext();
                    hasOldValues = oldEnum.MoveNext();
                }

                // the remaining nodes of the old destinations should be removed
                while (hasOldValues)
                {
                    removeDestinations.Add(oldEnum.Current);
                    hasOldValues = oldEnum.MoveNext();
                }

                // the remaining nodes of the new destinations should be added
                while (hasNewValues)
                {
                    addDestinations.Add(newEnum.Current);
                    hasNewValues = newEnum.MoveNext();
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
                    yield return member.NodeTag;
                }
                foreach (var promotable in Promotables)
                {
                    yield return promotable.NodeTag;
                }
            }
        }

        public IEnumerable<ReplicationNode> AllReplicationNodes()
        {
            foreach (var member in Members)
            {
                yield return member;
            }
            foreach (var promotable in Promotables)
            {
                yield return promotable;
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Members)] = new DynamicJsonArray(Members.Select(m => m.ToJson())),
                [nameof(Promotables)] = new DynamicJsonArray(Promotables.Select(p => p.ToJson())),
                [nameof(Watchers)] = Watchers.ToJsonWithConvertible()
            };
        }

        public void RemoveFromTopology(string delDbFromNode)
        {
            Members.RemoveAll(m => m.NodeTag == delDbFromNode);
            Promotables.RemoveAll(p => p.NodeTag == delDbFromNode);
        }

        public string WhoseTaskIsIt(IDatabaseTask task)
        {
            var topology = new List<DatabaseTopologyNode>(Members);
            topology.AddRange(Promotables);
            topology.Sort();


            if (topology.Count == 0)
                return null; // this is probably being deleted now, no one is able to run tasks

            var key = task.GetTaskKey();
            while (true)
            {
                var index = (int)Hashing.JumpConsistentHash.Calculate(key, topology.Count);
                var entry = topology[index];
                if (entry.Disabled == false && Members.Contains(entry))
                    return entry.NodeTag;

                topology.RemoveAt(index);

                // rehash so it will likely go to a different member in the cluster
                key = Hashing.Mix(key);
            }
        }

    }
}
