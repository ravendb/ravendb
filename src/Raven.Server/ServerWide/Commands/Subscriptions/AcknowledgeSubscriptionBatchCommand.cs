using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Replication;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class AcknowledgeSubscriptionBatchCommand : UpdateValueForDatabaseCommand
    {
        public string ChangeVector;
        public string LastKnownSubscriptionChangeVector;
        public long SubscriptionId;
        public string SubscriptionName;
        public string NodeTag;
        public string ShardName;
        public string ShardDbId;
        public string ShardLocalChangeVector;
        public bool HasHighlyAvailableTasks;
        public DateTime LastTimeServerMadeProgressWithDocuments;

        // for serialization
        private AcknowledgeSubscriptionBatchCommand() { }

        public AcknowledgeSubscriptionBatchCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override string GetItemId() => SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, SubscriptionName);

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, RawDatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            var subscriptionName = SubscriptionName;
            var isSharded = ShardName != null;
            if (string.IsNullOrEmpty(subscriptionName))
            {
                subscriptionName = SubscriptionId.ToString();
            }

            if (existingValue == null)
                throw new SubscriptionDoesNotExistException($"Subscription with name '{subscriptionName}' does not exist");

            var subscription = JsonDeserializationCluster.SubscriptionState(existingValue);
            var topology = ShardName == null ? record.Topology : record.Shards[ShardHelper.TryGetShardIndex(ShardName)];
            var lastResponsibleNode = GetLastResponsibleNode(HasHighlyAvailableTasks, topology, NodeTag);
            var appropriateNode = topology.WhoseTaskIsIt(RachisState.Follower, subscription, lastResponsibleNode);
            if (appropriateNode == null && record.DeletionInProgress.ContainsKey(NodeTag))
                throw new DatabaseDoesNotExistException($"Stopping subscription '{subscriptionName}' on node {NodeTag}, because database '{DatabaseName}' is being deleted.");

            if (appropriateNode != NodeTag)
            {
                throw new SubscriptionDoesNotBelongToNodeException(
                    $"Cannot apply {nameof(AcknowledgeSubscriptionBatchCommand)} for subscription '{subscriptionName}' with id '{SubscriptionId}', on database '{DatabaseName}', on node '{NodeTag}'," +
                    $" because the subscription task belongs to '{appropriateNode ?? "N/A"}'.")
                { AppropriateNode = appropriateNode };
            }

            if (ChangeVector == nameof(Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange))
            {
                return context.ReadObject(existingValue, SubscriptionName);
            }

            if (isSharded)
            {
                if (string.IsNullOrEmpty(LastKnownSubscriptionChangeVector))
                {
                    if (string.IsNullOrEmpty(subscription.ChangeVectorForNextBatchStartingPoint))
                    {
                        // can update
                    }
                    else
                    {
                        var currentLocalShardCv = ShardLocalChangeVector.ToChangeVector();
                        var currentCvInStorage = subscription.ChangeVectorForNextBatchStartingPoint.ToChangeVector();

                        foreach (var entry in currentLocalShardCv)
                        {
                            if (currentCvInStorage.Any(x => x.DbId == entry.DbId))
                                throw new SubscriptionChangeVectorUpdateConcurrencyException($"Can't acknowledge sharded subscription with name '{subscriptionName}' due to inconsistency in change vector progress. Probably there was an admin intervention that changed the change vector value. Stored value: {subscription.ChangeVectorForNextBatchStartingPoint}, received value: {LastKnownSubscriptionChangeVector}");
                        }
                    }
                }
                else
                {
                    if (string.IsNullOrEmpty(subscription.ChangeVectorForNextBatchStartingPoint))
                    {
                        throw new SubscriptionChangeVectorUpdateConcurrencyException($"Can't acknowledge sharded subscription with name '{subscriptionName}' due to inconsistency in change vector progress. Probably there was an admin intervention that changed the change vector value. Stored value: {subscription.ChangeVectorForNextBatchStartingPoint}, received value: {LastKnownSubscriptionChangeVector}");
                    }
                    else
                    {
                        var currentCvInStorage = subscription.ChangeVectorForNextBatchStartingPoint.ToChangeVector();
                        var currentCvInCommand = LastKnownSubscriptionChangeVector.ToChangeVector();
                        var currentLocalShardCv = ShardLocalChangeVector.ToChangeVector();

                        foreach (var entry in currentLocalShardCv)
                        {
                            var lastStorageCv = currentCvInStorage.FirstOrDefault(x => x.DbId == entry.DbId);
                            var lastCommandCv = currentCvInCommand.FirstOrDefault(x => x.DbId == entry.DbId);
                            if (lastStorageCv.Equals(lastCommandCv) == false)
                            {
                                throw new SubscriptionChangeVectorUpdateConcurrencyException($"Can't acknowledge sharded subscription with name '{subscriptionName}' due to inconsistency in change vector progress. Probably there was an admin intervention that changed the change vector value. Stored value: {subscription.ChangeVectorForNextBatchStartingPoint}, received value: {LastKnownSubscriptionChangeVector}");
                            }
                        }
                    }
                }

                subscription.ChangeVectorForNextBatchStartingPoint = ChangeVectorUtils.MergeVectors(ChangeVector, subscription.ChangeVectorForNextBatchStartingPoint);
            }
            else
            {
                if (LastKnownSubscriptionChangeVector != subscription.ChangeVectorForNextBatchStartingPoint)
                    throw new SubscriptionChangeVectorUpdateConcurrencyException($"Can't acknowledge subscription with name '{subscriptionName}' due to inconsistency in change vector progress. Probably there was an admin intervention that changed the change vector value. Stored value: {subscription.ChangeVectorForNextBatchStartingPoint}, received value: {LastKnownSubscriptionChangeVector}");

                subscription.ChangeVectorForNextBatchStartingPoint = ChangeVector;
            }

            subscription.NodeTag = NodeTag;
            subscription.LastBatchAckTime = LastTimeServerMadeProgressWithDocuments;

            return context.ReadObject(subscription.ToJson(), subscriptionName);
        }

        public static Func<string> GetLastResponsibleNode(
            bool hasHighlyAvailableTasks,
            DatabaseTopology topology,
            string nodeTag)
        {
            return () =>
            {
                if (hasHighlyAvailableTasks)
                    return null;

                if (topology.Members.Contains(nodeTag) == false)
                    return null;

                return nodeTag;
            };
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ChangeVector)] = ChangeVector;
            json[nameof(SubscriptionId)] = SubscriptionId;
            json[nameof(SubscriptionName)] = SubscriptionName;
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(HasHighlyAvailableTasks)] = HasHighlyAvailableTasks;
            json[nameof(LastTimeServerMadeProgressWithDocuments)] = LastTimeServerMadeProgressWithDocuments;
            json[nameof(LastKnownSubscriptionChangeVector)] = LastKnownSubscriptionChangeVector;
            json[nameof(ShardName)] = ShardName;
            json[nameof(ShardDbId)] = ShardDbId;
            json[nameof(ShardLocalChangeVector)] = ShardLocalChangeVector;
        }

        public override string AdditionalDebugInformation(Exception exception)
        {
            var msg = $"Got 'Ack' for id={SubscriptionId}, name={SubscriptionName}, CV={ChangeVector}, Tag={NodeTag}, lastProgressTime={LastTimeServerMadeProgressWithDocuments}" +
                $"lastKnownCV={LastKnownSubscriptionChangeVector}, HasHighlyAvailableTasks={HasHighlyAvailableTasks}.";
            if (exception != null)
            {
                msg += $" Exception = {exception}.";
            }

            return msg;
        }
    }
}
