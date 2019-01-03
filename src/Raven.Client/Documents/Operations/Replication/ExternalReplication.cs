using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Replication;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class ExternalReplication : ReplicationNode, IDatabaseTask, IDynamicJsonValueConvertible
    {
        public long TaskId;
        public string Name;
        public string ConnectionStringName;
        public string MentorNode;
        public TimeSpan DelayReplicationFor;

        [JsonDeserializationIgnore]
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

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(TaskId)] = TaskId;
            json[nameof(Name)] = Name;
            json[nameof(MentorNode)] = MentorNode;
            json[nameof(ConnectionStringName)] = ConnectionStringName;
            json[nameof(DelayReplicationFor)] = DelayReplicationFor;
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
                hashCode = (hashCode * 397) ^ CalculateStringHash(ConnectionStringName);
                return (int)hashCode;
            }
        }

        public override bool IsEqualTo(ReplicationNode other)
        {
            if (other is ExternalReplication externalReplication)
            {
                return string.Equals(ConnectionStringName, externalReplication.ConnectionStringName, StringComparison.OrdinalIgnoreCase) &&
                       TaskId == externalReplication.TaskId &&
                       DelayReplicationFor == externalReplication.DelayReplicationFor &&
                       string.Equals(externalReplication.Name, Name, StringComparison.OrdinalIgnoreCase) &&
                       string.Equals(externalReplication.Database, Database, StringComparison.OrdinalIgnoreCase);
            }
            return false;
        }

        public string GetMentorNode()
        {
            return MentorNode;
        }

        public string GetDefaultTaskName()
        {
            return $"External Replication to {ConnectionStringName}";
        }

        public string GetTaskName()
        {
            return Name;
        }
    }
}
