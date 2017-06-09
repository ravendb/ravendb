using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
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
        public string Script { get; set; }
        public bool IsVersioned { get; set; }
    }

    public class SubscriptionCriteria<T>
    {
        public string Script { get; set; }
        public bool? IsVersioned { get; set; }
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
            Criteria = new SubscriptionCriteria<T>();
        }

        public SubscriptionCriteria CreateOptions(DocumentConventions conventions)
        {
            var tType = typeof(T);
            var isVersioned = tType.IsConstructedGenericType && tType.GetGenericTypeDefinition() == typeof(Versioned<>);

            return new SubscriptionCriteria(conventions.GetCollectionName(isVersioned ? tType.GenericTypeArguments[0] : typeof(T)))
            {
                Script = Criteria?.Script ?? (isVersioned ? "return {Current:this.Current, Previous:this.Previous};" : null),
                IsVersioned =  isVersioned || (Criteria?.IsVersioned ?? false) 
            };

        }
        
        public SubscriptionCriteria<T> Criteria { get; set; }
        public ChangeVectorEntry[] ChangeVector { get; set; }
    }

    public class Versioned<T> where T : class
    {
        public T Previous;
        public T Current;
    }
}
