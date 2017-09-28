using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Replication;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide
{
    public class ExternalReplication : ReplicationNode, IDatabaseTask, IDynamicJsonValueConvertible
    {
        public long TaskId;
        public string Name;
        public string[] TopologyDiscoveryUrls;
        public string MentorNode;

        public ExternalReplication() { }

        public ExternalReplication(string database, string[] urls)
        {
            if(urls == null || urls.Length == 0)
                throw new ArgumentNullException(nameof(TopologyDiscoveryUrls));
            Database = database;
            TopologyDiscoveryUrls = urls;
            for (int i = 0; i < TopologyDiscoveryUrls.Length; i++)
            {
                if (TopologyDiscoveryUrls[i] == null)
                    throw new ArgumentNullException(nameof(TopologyDiscoveryUrls));

                TopologyDiscoveryUrls[i] = TopologyDiscoveryUrls[i].Trim();
            }
        }

        public static void RemoveWatcher(ref List<ExternalReplication> watchers, long taskId)
        {
            foreach (var watcher in watchers)
            {
                if (watcher.TaskId != taskId)
                    continue;
                watchers.Remove(watcher);
                return;
            }
        }
        
        public static void EnsureUniqueDbAndUrl(List<ExternalReplication> watchers, ExternalReplication watcher)
        {
            var dbName = watcher.Database;
            var url = watcher.Url;
            foreach (var w in watchers)
            {
                if (w.Database != dbName || w.Url != url)
                    continue;
                watchers.Remove(watcher);
                return;
            }
        }

        internal static (IEnumerable<ExternalReplication> AddedDestinations, IEnumerable<ExternalReplication> RemovedDestiantions) FindChanges(
            List<ExternalReplication> current, List<ExternalReplication> newDestinations)
        {
            if (current == null)
            {
                current = new List<ExternalReplication>();
            }
            if (newDestinations == null)
            {
                newDestinations = new List<ExternalReplication>();
            }

            return (newDestinations.Except(current), current.Except(newDestinations));
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(TaskId)] = TaskId;
            json[nameof(Name)] = Name;
            json[nameof(MentorNode)] = MentorNode;
            json[nameof(TopologyDiscoveryUrls)] = new DynamicJsonArray(TopologyDiscoveryUrls);
            return json;
        }

        public override string FromString()
        {
            return $"[{Database} @ {Url}]";
        }

        public ulong GetTaskKey()
        {
            var hashCode = CalculateStringHash(Database);
            hashCode = (hashCode * 397) ^ CalculateStringHash(string.Join(",", TopologyDiscoveryUrls));
            return hashCode;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = CalculateStringHash(Database);
                hashCode = (hashCode * 397) ^ CalculateStringHash(string.Join(",",TopologyDiscoveryUrls));
                hashCode = (hashCode * 397) ^ (ulong)TaskId;
                hashCode = (hashCode * 397) ^ CalculateStringHash(MentorNode);
                hashCode = (hashCode * 397) ^ CalculateStringHash(Name);
                return (int)hashCode;
            }
        }

        public string GetMentorNode()
        {
            return MentorNode;
        }
    }
}
