// -----------------------------------------------------------------------
//  <copyright file="SubscriptionCriteria.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionState : IDatabaseTask
    {
        public string Query { get; set; }
        public string ChangeVectorForNextBatchStartingPoint { get; set; }
        public long SubscriptionId { get; set; }
        public string SubscriptionName { get; set; }
        public DateTime LastTimeServerMadeProgressWithDocuments { get; set; }  // Last time server made some progress with the subscriptions docs  
        public DateTime LastClientConnectionTime { get; set; } // Last time any client has connected to server (connection dead or alive)
        public bool Disabled { get; set; }

        public ulong GetTaskKey()
        {
            return (ulong)SubscriptionId;
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Query)] = Query,
                [nameof(ChangeVectorForNextBatchStartingPoint)] = ChangeVectorForNextBatchStartingPoint,
                [nameof(SubscriptionId)] = SubscriptionId,
                [nameof(SubscriptionName)] = SubscriptionName,
                [nameof(LastTimeServerMadeProgressWithDocuments)] = LastTimeServerMadeProgressWithDocuments,
                [nameof(LastClientConnectionTime)] = LastClientConnectionTime,
                [nameof(Disabled)] = Disabled
            };
        }

        public static string GenerateSubscriptionItemKeyName(string databaseName, string subscriptionName)
        {
            return $"{SubscriptionPrefix(databaseName)}{subscriptionName}";
        }

        public static string SubscriptionPrefix(string databaseName)
        {
            return $"{Helpers.ClusterStateMachineValuesPrefix(databaseName)}subscriptions/";
        }
    }

    public class SubscriptionStateWithNodeDetails : SubscriptionState
    {
        public NodeId ResponsibleNode { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(ResponsibleNode)] = ResponsibleNode;
            return json;
        }
    }
}
