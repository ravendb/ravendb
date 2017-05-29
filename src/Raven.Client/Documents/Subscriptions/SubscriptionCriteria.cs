using System;
using System.Collections.Generic;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Extensions;
using Raven.Client.Server;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionCriteria 
    {
        public SubscriptionCriteria()
        {
            // for deserialization
        }

        public SubscriptionCriteria(string collection)
        {
            Collection = collection ?? throw new ArgumentNullException(nameof(collection));
        }

        public string Collection { get;  set; }
        public string FilterJavaScript { get; set; }
    }

    public class SubscriptionCriteria<T>
    {
        public SubscriptionCriteria()
        {

        }
        public string FilterJavaScript { get; set; }
    }

    public class SubscriptionCreationOptions
    {
        public SubscriptionCriteria Criteria { get; set; }
        public ChangeVectorEntry[] ChangeVector { get; set; }
    }

    public class SubscriptionCreationOptions<T>
    {
        public SubscriptionCreationOptions()
        {

        }
        public SubscriptionCriteria<T> Criteria { get; set; }
        public ChangeVectorEntry[] ChangeVector { get; set; }
    }
}
