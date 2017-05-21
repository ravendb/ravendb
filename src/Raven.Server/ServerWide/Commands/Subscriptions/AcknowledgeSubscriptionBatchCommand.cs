using System;
using System.Collections.Generic;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Server;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class AcknowledgeSubscriptionBatchCommand : UpdateValueForDatabaseCommand, IDatabaseTask
    {
        public ChangeVectorEntry[] ChangeVector;
        public string SubscriptionId;
        public string NodeTag;
        public Guid DbId;
        public long LastDocumentEtagAckedInNode;

        // for serializtion
        private AcknowledgeSubscriptionBatchCommand() : base(null) { }

        public AcknowledgeSubscriptionBatchCommand(string databaseName) : base(databaseName)
        {
        }

        public override string GetItemId() => SubscriptionId;

        public override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            if (existingValue == null)
                throw new InvalidOperationException($"Subscription with id {SubscriptionId} does not exist");

            if (record.Topology.WhoseTaskIsIt(this) != NodeTag)
                throw new InvalidOperationException($"Can't update subscription with id {SubscriptionId} by node {NodeTag}, because it's not it's task to update this subscription");

            var subscription = JsonDeserializationCluster.SubscriptionState(existingValue);
            
            if (subscription.LastEtagReachedInServer == null)
                subscription.LastEtagReachedInServer = new Dictionary<string, long>();
            subscription.LastEtagReachedInServer[DbId.ToString()] = LastDocumentEtagAckedInNode;
            subscription.ChangeVector = ChangeVector;
            subscription.TimeOfLastClientActivity = DateTime.UtcNow;

            return context.ReadObject(subscription.ToJson(), GetItemId());
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ChangeVector)] = ChangeVector?.ToJson();
            json[nameof(SubscriptionId)] = SubscriptionId;
            json[nameof(NodeTag)] = NodeTag;
        }

        private ulong? _taskKey;

        public ulong GetTaskKey()
        {
            if (_taskKey.HasValue == false)
            {
                var lastSlashIndex = SubscriptionId.LastIndexOf("/", StringComparison.OrdinalIgnoreCase);
                _taskKey = ulong.Parse(SubscriptionId.Substring(lastSlashIndex + 1));
                return _taskKey.Value;
            }
            return _taskKey.Value;
        }
    }
}
