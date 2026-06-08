using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

public enum AiMessageRole
{
    System,
    User,
    Assistant,
    Summary,
    Internal
}

public class AiToolCallResult : IDynamicJson
{
    /// <summary>
    /// The tool call ID from the model.
    /// </summary>
    public string Id { get; set; }

    /// <summary>
    /// Tool name.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// Arguments the model passed, as JSON string.
    /// </summary>
    public string Arguments { get; set; }

    /// <summary>
    /// The tool's response content. Null if still pending (ActionRequired).
    /// </summary>
    public string Result { get; set; }

    /// <summary>
    /// If this tool call was a sub-agent invocation, the ID of the spawned sub-conversation.
    /// Can be queried separately via GetConversationMessages.
    /// </summary>
    public string SubConversationId { get; set; }

    /// <summary>
    /// Serializes this tool-call result to a JSON structure.
    /// </summary>
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Id)] = Id,
            [nameof(Name)] = Name,
            [nameof(Arguments)] = Arguments,
            [nameof(Result)] = Result,
            [nameof(SubConversationId)] = SubConversationId
        };
    }
}

public class AiConversationMessage : IDynamicJson
{
    /// <summary>
    /// The role of the message sender.
    /// </summary>
    public AiMessageRole Role { get; set; }

    /// <summary>
    /// Text content. When the stored message has multiple text parts, they are
    /// joined with line breaks. Null for assistant messages that only initiated tool calls.
    /// </summary>
    public string Content { get; set; }

    /// <summary>
    /// Attachment file names associated with this message, if any.
    /// </summary>
    public List<string> Attachments { get; set; }

    /// <summary>
    /// When this message was recorded (UTC). Guaranteed unique and monotonic
    /// within a conversation — safe to use as a paging cursor.
    /// </summary>
    public DateTime Timestamp { get; set; }

    /// <summary>
    /// Tool calls initiated by this assistant message, with their responses inlined.
    /// Empty when no tool calls are present (including for non-assistant messages).
    /// </summary>
    public List<AiToolCallResult> ToolCalls { get; set; }

    /// <summary>
    /// Token usage for this message (typically on assistant messages).
    /// </summary>
    public AiUsage Usage { get; set; }

    /// <summary>
    /// For Internal role messages: the ID of the sub-conversation this message relates to.
    /// </summary>
    public string SubConversationId { get; set; }

    /// <summary>
    /// Serializes this message to a JSON structure.
    /// </summary>
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Role)] = Role.ToString(),
            [nameof(Content)] = Content,
            [nameof(Attachments)] = Attachments != null ? new DynamicJsonArray(Attachments) : null,
            [nameof(Timestamp)] = Timestamp,
            [nameof(ToolCalls)] = ToolCalls != null ? new DynamicJsonArray(ToolCalls) : null,
            [nameof(Usage)] = Usage?.ToJson(),
            [nameof(SubConversationId)] = SubConversationId
        };
    }
}
