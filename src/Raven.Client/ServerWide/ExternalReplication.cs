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
            if(string.IsNullOrEmpty(connectionStringName))
                throw new ArgumentNullException(nameof(connectionStringName));

            if (string.IsNullOrEmpty(database))
                throw new ArgumentNullException(nameof(database));

            Database = database;
            ConnectionStringName = connectionStringName;
        }

        public static void RemoveWatcher(List<ExternalReplication> watchers, long taskId)
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
            var connecitonString = watcher.ConnectionStringName;
            foreach (var w in watchers)
            {
                if (w.ConnectionStringName != connecitonString)
                    continue;
                watchers.Remove(watcher);
                return;
            }
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
                var hashCode = (ulong)base.GetHashCode();
                hashCode = (hashCode * 397) ^ CalculateStringHash(Name);
                hashCode = (hashCode * 397) ^ CalculateStringHash(MentorNode);
                hashCode = (hashCode * 397) ^ CalculateStringHash(ConnectionStringName);
                return (int)hashCode;
            }
        }

        public override bool IsEqualTo(ReplicationNode other)
        {
            var externalReplication = (ExternalReplication)other;
            return base.IsEqualTo(other) && 
                   string.Equals(Name, externalReplication.Name, StringComparison.OrdinalIgnoreCase) &&
                   string.Equals(MentorNode, externalReplication.MentorNode, StringComparison.OrdinalIgnoreCase) &&
                   string.Equals(ConnectionStringName, externalReplication.ConnectionStringName, StringComparison.OrdinalIgnoreCase);
        }

        public string GetMentorNode()
        {
            return MentorNode;
        }
    }
}
