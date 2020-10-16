using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Replication;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public abstract class ExternalReplicationBase : ReplicationNode, IDatabaseTask, IDynamicJsonValueConvertible
    {
        public long TaskId
        {
            get => _taskId;
            set
            {
                if (HashCodeSealed)
                {
                    throw new InvalidOperationException(
                        $"TaskId of 'ExternalReplicationBase' can't be modified after 'GetHashCode' was invoked, if you see this error it is likley a bug (taskId={_taskId} value={value} Url={Url}).");
                }

                _taskId = value;
            }
        }

        public string Name { get; set; }
        public string ConnectionStringName { get; set; }
        public string MentorNode { get; set; }

        [JsonDeserializationIgnore]
        internal RavenConnectionString ConnectionString; // this is in memory only

        private long _taskId;

        protected ExternalReplicationBase()
        {
        }

        protected ExternalReplicationBase(string database, string connectionStringName)
        {
            if (string.IsNullOrEmpty(connectionStringName))
                throw new ArgumentNullException(nameof(connectionStringName));

            if (string.IsNullOrEmpty(database))
                throw new ArgumentNullException(nameof(database));

            Database = database;
            ConnectionStringName = connectionStringName;
        }

        public void AssertValidReplication()
        {
            if (string.IsNullOrEmpty(ConnectionStringName))
                throw new ArgumentNullException(nameof(ConnectionStringName));
        }

        public static void RemoveExternalReplication<T>(List<T> replicationTasks, long taskId) where T : ExternalReplicationBase
        {
            foreach (var task in replicationTasks)
            {
                if (task.TaskId != taskId)
                    continue;
                replicationTasks.Remove(task);
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

        public virtual ulong GetTaskKey()
        {
            var hashCode = CalculateStringHash(Database);
            hashCode = (hashCode * 397) ^ CalculateStringHash(ConnectionStringName);
            return (hashCode * 397) ^ (ulong)TaskId;
        }

        public override int GetHashCode()
        {
            var hashCode = TaskId.GetHashCode();
            HashCodeSealed = true;
            return hashCode;
        }

        public override bool IsEqualTo(ReplicationNode other)
        {
            if (other is ExternalReplicationBase externalReplication)
            {
                return string.Equals(ConnectionStringName, externalReplication.ConnectionStringName, StringComparison.OrdinalIgnoreCase) &&
                       string.Equals(externalReplication.Database, Database, StringComparison.OrdinalIgnoreCase) &&
                       TaskId == externalReplication.TaskId;
            }

            return false;
        }

        public bool Equals(ExternalReplicationBase other) => IsEqualTo(other);

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return IsEqualTo((ExternalReplicationBase)obj);
        }

        public string GetMentorNode()
        {
            return MentorNode;
        }

        public virtual string GetDefaultTaskName()
        {
            return $"External Replication to {ConnectionStringName}";
        }

        public string GetTaskName()
        {
            return Name;
        }

        public bool IsResourceIntensive()
        {
            return false;
        }
    }
}
