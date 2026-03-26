using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents
{
    /// <summary>
    /// Defines configuration options for reducing the size of the AI agent's chat history.
    /// </summary>
    /// <remarks>
    /// Only one trimming strategy can be active at a time: either summarization or truncation.
    /// This is used to control memory and token limits by either summarizing or discarding older messages.
    /// </remarks>
    public class AiAgentChatTrimmingConfiguration
    {
        public AiAgentChatTrimmingConfiguration()
        {
            // for serialization
        }

        /// <summary>
        /// Initializes a new <see cref="AiAgentChatTrimmingConfiguration"/> using a summarization strategy.
        /// </summary>
        /// <param name="tokensConfig">
        /// The settings that control how and when the chat history is summarized into a concise prompt
        /// (e.g. token thresholds, prompt prefix, etc.).
        /// </param>
        /// <param name="historyConfig">
        /// Optional. Configuration for persisting the chat history when summarization occurs.
        /// If <c>null</c>, no history documents will be created.
        /// </param>
        /// <remarks>
        /// Only one trimming strategy may be active at a time. Do not use this constructor in conjunction
        /// with the truncation-based constructor.
        /// </remarks>
        public AiAgentChatTrimmingConfiguration(AiAgentSummarizationByTokens tokensConfig, AiAgentHistoryConfiguration historyConfig = null)
        {
            Tokens = tokensConfig;
            History = historyConfig;
        }

        /// <summary>
        /// Initializes a new <see cref="AiAgentChatTrimmingConfiguration"/> using a truncation strategy.
        /// </summary>
        /// <param name="truncateConfig">
        /// The settings that control how and when older messages are discarded once the message count
        /// exceeds the configured maximum.
        /// </param>
        /// <param name="historyConfig">
        /// Optional. Configuration for persisting the chat history when truncation occurs.
        /// If <c>null</c>, no history documents will be created.
        /// </param>
        /// <remarks>
        /// Only one trimming strategy may be active at a time. Do not use this constructor in conjunction
        /// with the summarization-based constructor.
        /// </remarks>
        internal AiAgentChatTrimmingConfiguration(AiAgentTruncateChat truncateConfig, AiAgentHistoryConfiguration historyConfig = null)
        {
            Truncate = truncateConfig;
            History = historyConfig;
        }

        /// <summary>
        /// Summarizes chat messages into a compact prompt when token count exceeds a threshold.
        /// Cannot be used together with <see cref="Truncate"/>.
        /// </summary>
        public AiAgentSummarizationByTokens Tokens { get; set; }

        /// <summary>
        /// Truncates older chat messages when the number of messages exceeds a maximum length.
        /// Cannot be used together with <see cref="Tokens"/>.
        /// </summary>
        [ForceJsonSerialization]
        internal AiAgentTruncateChat Truncate { get; set; }

        /// <summary>
        /// Configuration settings for storing AI agent conversation history.
        /// </summary>
        /// <remarks>
        /// If this field is <c>null</c>, no history documents will be created or saved
        /// for the chat when a trimming operation (truncation or summarization) occurs.
        /// </remarks>
        public AiAgentHistoryConfiguration History { get; set; }

        /// <summary>
        /// Serializes the configuration to a JSON structure.
        /// </summary>
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Tokens)] = Tokens?.ToJson(),
                [nameof(Truncate)] = Truncate?.ToJson(),
                [nameof(History)] = History?.ToJson()
            };
        }
    }

    /// <summary>
    /// Configuration settings for AI agent conversation summarization.
    /// </summary>
    /// <remarks>
    /// Defines how and when the AI agent should summarize the conversation history
    /// into a concise system prompt to continue the dialogue.
    /// </remarks>
    public class AiAgentSummarizationByTokens
    {
        /// <summary>
        /// Instruction text prepended to the serialized conversation when requesting a summary.
        /// </summary>
        /// <value>
        /// A detailed directive that provides context, formatting guidance, and any special requirements
        /// for how the AI should summarize the preceding chat history.
        /// </value>
        /// <remarks>
        /// This prompt is sent with the <c>system</c> role and appears before any summary content.
        /// Customize it to influence the structure, tone, or depth of the generated summary.
        /// </remarks>
        public string SummarizationTaskBeginningPrompt { get; set; }

        /// <summary>
        /// The user-role message that triggers the summarization process.
        /// </summary>
        /// <value>
        /// A concise reminder instructing the AI to review the entire prior conversation
        /// and produce a summary according to the guidelines defined in <see cref="SummarizationTaskBeginningPrompt"/>.
        /// </value>
        /// <remarks>
        /// This prompt is sent immediately after <c>SystemPrompt</c> and represents the actual
        /// request to generate the summary. Adjust its phrasing to change how explicitly
        /// the model understands the summarization task.
        /// </remarks>
        public string SummarizationTaskEndPrompt { get; set; }

        /// <summary>
        /// The text prefix that appears before the generated summary of the previous conversation.
        /// </summary>
        /// <value>
        /// A <see cref="string"/> containing the introductory label for the summary output.  
        /// </value>
        /// <remarks>
        /// Customize this prefix to change how the summary is introduced when producing conversation summaries.
        /// </remarks>
        public string ResultPrefix { get; set; }

        /// <summary>
        /// The maximum number of tokens allowed before summarization is triggered.
        /// When the token count of the conversation exceeds this limit, the content will be summarized.
        /// </summary>
        public long? MaxTokensBeforeSummarization { get; set; }

        /// <summary>
        /// Maximum number of tokens allowed in the generated summary.
        /// </summary>
        /// <value>
        /// The upper bound on the summary’s length, measured in tokens( default is <c>1024</c>).
        /// </value>
        public long? MaxTokensAfterSummarization { get; set; }

        /// <summary>
        /// Converts the summarization configuration into a JSON representation.
        /// </summary>
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(SummarizationTaskBeginningPrompt)] = SummarizationTaskBeginningPrompt,
                [nameof(SummarizationTaskEndPrompt)] = SummarizationTaskEndPrompt,
                [nameof(ResultPrefix)] = ResultPrefix,
                [nameof(MaxTokensBeforeSummarization)] = MaxTokensBeforeSummarization,
                [nameof(MaxTokensAfterSummarization)] = MaxTokensAfterSummarization
            };
        }
    }

    /// <summary>
    /// Configuration for truncating the AI chat history based on message count.
    /// </summary>
    /// <remarks>
    /// When the number of chat messages exceeds the specified maximum, the oldest messages are removed
    /// and optionally archived into a separate history section.
    /// </remarks>
    public class AiAgentTruncateChat
    {
        private const int DefaultMessagesLengthBeforeTruncate = 500;

        /// <summary>
        /// Maximum number of messages allowed before delete the old messages
        /// </summary>
        public int MessagesLengthBeforeTruncate { get; set; } = DefaultMessagesLengthBeforeTruncate;

        /// <summary>
        /// Number of messages after delete the old messages
        /// </summary>
        public int MessagesLengthAfterTruncate { get; set; } = DefaultMessagesLengthBeforeTruncate / 2;

        /// <summary>
        /// Converts the truncation configuration into a JSON representation.
        /// </summary>
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(MessagesLengthBeforeTruncate)] = MessagesLengthBeforeTruncate,
                [nameof(MessagesLengthAfterTruncate)] = MessagesLengthAfterTruncate
            };
        }
    }

    /// <summary>
    /// Defines the configuration for retention and expiration of AI agent chat history documents.
    /// </summary>
    /// <remarks>
    /// History documents are stored copies of chats before they have been summarized or truncated.
    /// If an instance of this class is <c>null</c> (i.e. the containing <see cref="History"/> field is <c>null</c>),
    /// no history documents will be created or persisted when a trimming operation (truncation or summarization) occurs.
    /// </remarks>
    public class AiAgentHistoryConfiguration
    {
        /// <summary>
        /// Enables history for the AI agents conversations.
        /// </summary>
        public AiAgentHistoryConfiguration()
        {
            
        }

        /// <summary>
        /// Enables history for the AI agents conversations.
        /// </summary>
        /// <param name="expiration">The timespan after which history documents expire.</param>
        public AiAgentHistoryConfiguration(TimeSpan expiration)
        {
            HistoryExpirationInSec = (int)expiration.TotalSeconds;
        }

        /// <summary>
        /// The timespan after which history documents expire.
        /// </summary>
        /// <remarks>
        /// History documents are the retained copies of chat messages that have been summarized or truncated.
        /// This property defines how long those history documents will be kept before they are considered expired and eligible for removal.
        /// </remarks>
        public int? HistoryExpirationInSec { get; set; }

        /// <summary>
        /// Converts the history configuration into a JSON representation.
        /// </summary>
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(HistoryExpirationInSec)] = HistoryExpirationInSec
            };
        }
    }
}
