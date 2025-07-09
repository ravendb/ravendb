using System;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents
{
    /// <summary>
    /// Configuration for persisting chat history in RavenDB.
    /// Defines where chat sessions should be stored and optionally how long they should be retained (expiration).
    /// </summary>
    public class AiAgentPersistenceConfiguration : IDynamicJson
    {
        public AiAgentPersistenceConfiguration()
        {
            // for serialization
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AiAgentPersistenceConfiguration"/> class
        /// with the specified target collection and optional expiration.
        /// </summary>
        /// <param name="collection">
        /// The name of the database collection where chat session documents should be stored.
        /// This is typically a collection like "Chats" or "Conversations".
        /// </param>
        /// <param name="expires">
        /// Optional expiration duration. If provided, chat documents will expire (and be deleted)
        /// automatically after this time has passed since creation.
        /// </param>
        public AiAgentPersistenceConfiguration(string collection, TimeSpan? expires = null)
        {
            ValidationMethods.AssertNotNullOrEmpty(collection, nameof(collection));

            Collection = collection;
            Expires = expires;
        }

        /// <summary>
        /// The name of the database collection where chat session documents should be stored.
        /// This is typically a collection like "Chats" or "Conversations".
        /// This allows separation between different types of persisted AI conversations.
        /// </summary>
        public string Collection { get; set; }

        /// <summary>
        /// Optional expiration duration. If provided, chat documents will expire (and be deleted)
        /// automatically after this time has passed since creation.
        /// </summary>
        public TimeSpan? Expires { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue { [nameof(Collection)] = Collection, [nameof(Expires)] = Expires?.TotalMilliseconds };
        }
    }
}
