using System;
using System.Collections.Generic;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Extensions;
using Raven.Client.Server;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionCriteria : IFillFromBlittableJson
    {
        public SubscriptionCriteria()
        {
            // for deserialization
        }

        public SubscriptionCriteria(string collection)
        {
            Collection = collection ?? throw new ArgumentNullException(nameof(collection));
        }

        public string Collection { get; private set; }
        public string FilterJavaScript { get; set; }

        public void FillFromBlittableJson(BlittableJsonReaderObject json)
        {
            if (json == null)
                return;

            string collection;
            if (json.TryGet(nameof(Collection), out collection))
                Collection = collection;

            string filterJavaScript;
            if (json.TryGet(nameof(FilterJavaScript), out filterJavaScript))
                FilterJavaScript = filterJavaScript;
        }
    }

    public class SubscriptionCriteria<T>
    {
        public SubscriptionCriteria()
        {

        }
        public string FilterJavaScript { get; set; }
    }

    public class SubscriptionCreationParams
    {
        public SubscriptionCriteria Criteria { get; set; }
        public ChangeVectorEntry[] ChangeVector { get; set; }
    }

    public class SubscriptionCreationParams<T>
    {
        public SubscriptionCreationParams()
        {

        }
        public SubscriptionCriteria<T> Criteria { get; set; }
        public ChangeVectorEntry[] ChangeVector { get; set; }
    }
}
