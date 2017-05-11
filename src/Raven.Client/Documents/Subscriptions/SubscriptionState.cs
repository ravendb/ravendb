// -----------------------------------------------------------------------
//  <copyright file="SubscriptionCriteria.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionState:IFillFromBlittableJson, IDatabaseTask
    {
        public SubscriptionState()
        {

        }

        public SubscriptionCriteria Criteria { get; set; } 
        public ChangeVectorEntry[] ChangeVector { get; set; }
        public long SubscriptionId { get; set; }
        public DateTime TimeOfLastClientActivity { get; set; }
        public Dictionary<Guid,long> LastEtagReachedInServer { get; set; }

        public ulong GetTaskKey()
        {
            return (ulong)SubscriptionId;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(this.Criteria)] = new DynamicJsonValue
                {
                    [nameof(SubscriptionCriteria.Collection)] = Criteria.Collection,
                    [nameof(SubscriptionCriteria.FilterJavaScript)] = Criteria.FilterJavaScript
                },
                [nameof(ChangeVector)] = ChangeVector?.ToJson(),
                [nameof(SubscriptionId)] = SubscriptionId,
                [nameof(TimeOfLastClientActivity)] = TimeOfLastClientActivity
            };
        }

        public void FillFromBlittableJson(BlittableJsonReaderObject json)
        {
            if (json == null)
                return;

            long subscriptionId;
            if (json.TryGet(nameof(SubscriptionId), out subscriptionId))
                SubscriptionId = subscriptionId;


            if (json.TryGet(nameof(ChangeVector), out BlittableJsonReaderArray changeVector))
            {
                ChangeVector = changeVector.ToVector();
            }

            DateTime timeOfLastClientActivity;
            if (json.TryGet(nameof(TimeOfLastClientActivity), out timeOfLastClientActivity))
                TimeOfLastClientActivity = timeOfLastClientActivity;

            BlittableJsonReaderObject criteria;
            if (json.TryGet(nameof(Criteria), out criteria))
            {
                Criteria = new SubscriptionCriteria(Constants.Documents.Collections.AllDocumentsCollection);
                Criteria.FillFromBlittableJson(criteria);
            }
        }

        public static string GenerateSubscriptionItemName(string databaseName, long subscriptionId)
        {
            return $"subscriptions/{databaseName}/{subscriptionId}";
        }

        public static string GenerateSubscriptionPrefix(string databaseName)
        {
            return $"subscriptions/{databaseName}";
        }
    }
}