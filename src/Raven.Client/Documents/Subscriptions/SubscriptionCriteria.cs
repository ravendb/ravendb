// -----------------------------------------------------------------------
//  <copyright file="SubscriptionCriteria.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Sparrow.Json;

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionCriteria : IFillFromBlittableJson
    {
        private SubscriptionCriteria()
        {
            // for deserialization
        }

        public SubscriptionCriteria(string collection)
        {
            if (collection == null)
                throw new ArgumentNullException(nameof(collection));

            Collection = collection;
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
        public string FilterJavaScript { get; set; }
    }
}