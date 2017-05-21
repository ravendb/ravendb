// -----------------------------------------------------------------------
//  <copyright file="SubscriptionCriteria.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Extensions;
using Raven.Client.Server;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionState : IDatabaseTask
    {
        public SubscriptionState()
        {

        }

        public SubscriptionCriteria Criteria { get; set; }
        public ChangeVectorEntry[] ChangeVector { get; set; }
        public string SubscriptionId { get; set; }
        public DateTime TimeOfLastClientActivity { get; set; }
        public Dictionary<string, long> LastEtagReachedInServer { get; set; }
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