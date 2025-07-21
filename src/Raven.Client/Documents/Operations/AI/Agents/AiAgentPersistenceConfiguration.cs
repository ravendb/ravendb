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
        /// <param name="conversationIdPrefix">
        /// The prefix of the conversation ID.
        /// This is typically like "chats/" or "conversations/".
        /// </param>
        /// <param name="expires">
        /// Optional expiration duration. If provided, chat documents will expire (and be deleted)
        /// automatically after this time has passed since creation.
        /// </param>
        public AiAgentPersistenceConfiguration(string conversationIdPrefix = null, TimeSpan? expires = null)
        {
            ConversationIdPrefix = conversationIdPrefix;
            if (expires.HasValue)
            {
                ConversationExpirationInSec = (int)expires.Value.TotalSeconds;
            }
        }

        /// <summary>
        /// The prefix of the conversation ID.
        /// This is typically like "chats/" or "conversations/".
        /// This allows separation between different types of persisted AI conversations.
        /// </summary>
        public string ConversationIdPrefix { get; set; }

        /// <summary>
        /// Optional expiration duration. If provided, chat documents will expire (and be deleted)
        /// automatically after this time has passed since creation.
        /// </summary>
        public int? ConversationExpirationInSec { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ConversationIdPrefix)] = ConversationIdPrefix, 
                [nameof(ConversationExpirationInSec)] = ConversationExpirationInSec
            };
        }
    }
}
