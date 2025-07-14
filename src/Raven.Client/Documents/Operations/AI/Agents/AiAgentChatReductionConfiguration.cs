using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents
{
    /// <summary>
    /// Defines configuration options for reducing the size of the AI agent's chat history.
    /// </summary>
    /// <remarks>
    /// Only one reduction strategy can be active at a time: either summarization or truncation.
    /// This is used to control memory and token limits by either summarizing or discarding older messages.
    /// </remarks>
    public class AiAgentChatReductionConfiguration
    {
        private AiAgentSummarizationByTokens _tokens;
        private AiAgentTruncateChat _truncate;

        /// <summary>
        /// Summarizes chat messages into a compact prompt when token count exceeds a threshold.
        /// Cannot be used together with <see cref="Truncate"/>.
        /// </summary>
        public AiAgentSummarizationByTokens Tokens
        {
            get => _tokens;
            set
            {
                if (Truncate is not null)
                    throw new InvalidOperationException($"Cannot set {nameof(Tokens)} when {nameof(Truncate)} is already configured.");
                _tokens = value;
            }
        }

        /// <summary>
        /// Truncates older chat messages when the number of messages exceeds a maximum length.
        /// Cannot be used together with <see cref="Tokens"/>.
        /// </summary>
        public AiAgentTruncateChat Truncate
        {
            get => _truncate;
            set
            {
                if (Truncate is not null)
                    throw new InvalidOperationException($"Cannot set {nameof(Truncate)} when {nameof(Tokens)} is already configured.");
                _truncate = value;
            }
        }

        /// <summary>
        /// Configuration settings for storing AI agent conversation history.
        /// </summary>
        /// <remarks>
        /// If this field is <c>null</c>, no history documents will be created or saved
        /// for the chat when a reduction operation (truncation or summarization) occurs.
        /// </remarks>
        public AiAgentHistoryConfiguration History;

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
        private const int DefaultMaxTokensBeforeSummarization = 32 * 1024;

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
        /// The maximum number of tokens allowed before summarization is triggered.
        /// When the token count of the conversation exceeds this limit, the content will be summarized.
        /// </summary>
        public long MaxTokensBeforeSummarization { get; set; } = DefaultMaxTokensBeforeSummarization;

        /// <summary>
        /// Maximum number of tokens allowed in the generated summary.
        /// </summary>
        /// <value>
        /// The upper bound on the summary’s length, measured in tokens (default is <c>500</c>).
        /// </value>
        public long MaxTokensAfterSummarization { get; set; } = DefaultMaxTokensBeforeSummarization / 10;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(SummarizationTaskBeginningPrompt)] = SummarizationTaskBeginningPrompt,
                [nameof(SummarizationTaskEndPrompt)] = SummarizationTaskEndPrompt,
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
    /// no history documents will be created or persisted when a reduction operation (truncation or summarization) occurs.
    /// </remarks>
    public class AiAgentHistoryConfiguration
    {
        /// <summary>
        /// The timespan after which history documents expire.
        /// </summary>
        /// <remarks>
        /// History documents are the retained copies of chat messages that have been summarized or truncated.
        /// This property defines how long those history documents will be kept before they are considered expired and eligible for removal.
        /// </remarks>
        public TimeSpan? HistoryExpiration { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(HistoryExpiration)] = HistoryExpiration?.TotalMilliseconds
            };
        }
    }
}
