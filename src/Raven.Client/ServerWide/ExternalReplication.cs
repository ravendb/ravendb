using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Replication;
using Raven.Client.ServerWide.ETL;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide
{
    public class ExternalReplication : ReplicationNode, IDatabaseTask, IDynamicJsonValueConvertible
    {
        public long TaskId;
        public string Name;
        public string ConnectionStringName;
        public string MentorNode;

        [JsonIgnore]
        public RavenConnectionString ConnectionString; // this is in memory only

        public ExternalReplication() { }

        public ExternalReplication(string database, string connectionStringName)
        {
            Database = database;
            ConnectionStringName = connectionStringName;
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
        
        public static void EnsureUniqueDbAndConnectionString(List<ExternalReplication> watchers, ExternalReplication watcher)
        {
            var dbName = watcher.Database;
            var connecitonString = watcher.ConnectionStringName;
            foreach (var w in watchers)
            {
                if (w.Database != dbName || w.ConnectionStringName != connecitonString)
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
            json[nameof(ConnectionStringName)] = ConnectionStringName;
            return json;
        }

        public override string FromString()
        {
            return $"[{Database} @ {Url}]";
        }

        public ulong GetTaskKey()
        {
            var hashCode = CalculateStringHash(Database);
            hashCode = (hashCode * 397) ^ CalculateStringHash(ConnectionStringName);
            return (hashCode * 397) ^ (ulong)TaskId;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = CalculateStringHash(Database);
                hashCode = (hashCode * 397) ^ CalculateStringHash(ConnectionStringName);
                hashCode = (hashCode * 397) ^ (ulong)TaskId;
                hashCode = (hashCode * 397) ^ CalculateStringHash(MentorNode);
                return (int)hashCode;
            }
        }

        public string GetMentorNode()
        {
            return MentorNode;
        }
    }
}
