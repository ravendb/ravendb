using System;
using System.Collections.Generic;
using System.Text;
using NuGet.Packaging;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal sealed partial class AiAgentProcessorForGetConversationMessages
{
    /// <summary>
    /// Collects, groups, and filters messages across the current doc and history docs.
    /// Tool responses are accumulated into a dictionary and merged into their
    /// parent assistant message's ToolCalls during parsing.
    /// </summary>
    private sealed class Collector
    {
        private readonly DocumentsOperationContext _context;
        private readonly DocumentsStorage _storage;
        private readonly ConversationDocument _conversation;
        private readonly int _pageSize;
        private readonly AiConversationDetailLevel _detailLevel;
        private readonly bool _forward;
        private readonly DateTime _before;
        private readonly DateTime _after;

        private readonly List<AiConversationMessage> _results = new();
        private readonly Dictionary<string, (string Content, string SubConversationId)> _toolResponses = new(StringComparer.Ordinal);
        private readonly HashSet<(long TimestampTicks, string Role, string ToolCallId, int Hash)> _seenMessageKeys = new();
        private readonly HashSet<string> _attachmentNames = new(StringComparer.Ordinal);
        private bool _hasMoreMessages;

        public Collector(DocumentsOperationContext context, DocumentsStorage storage, ConversationDocument conversation,
            int pageSize, AiConversationDetailLevel detailLevel, DateTime? before, DateTime? after)
        {
            _context = context;
            _storage = storage;
            _conversation = conversation;
            _pageSize = pageSize;
            _detailLevel = detailLevel;
            _forward = after.HasValue;
            _after = after ?? DateTime.MinValue;
            _before = before ?? DateTime.MaxValue;
        }

        public bool HasMoreMessages => _hasMoreMessages;
        public HashSet<string> AttachmentNames => _attachmentNames;

        public List<AiConversationMessage> GetResults()
        {
            _results.Sort((a, b) => a.Timestamp.CompareTo(b.Timestamp));
            return _results;
        }

        /// <summary>
        /// Main entry point: collects messages from the current doc and history docs
        /// in the correct order for the paging direction.
        /// </summary>
        public void Collect()
        {
            var (histStart, histEnd) = FindRelevantHistoryRange();

            if (_forward)
            {
                // forward - old messages to new

                // Trimming moves older messages from the current doc into history, so the
                // chronologically earliest messages matching `after` may live in history docs.
                // Process history first (oldest to newest), then current doc.
                for (int i = histStart; i <= histEnd && NeedsMore; i++)
                    CollectFromHistoryDoc(_conversation.LinkedConversations[i]);

                if (NeedsMore)
                {
                    // we finished passing the history docs -> we collect also from the origin doc
                    CollectFromMessages(_conversation.Messages);
                }
                else
                    _hasMoreMessages = true; // if we don't visit the origin doc - we have more
            }
            else
            {
                // backward - new messages to old

                // For backward paging, current doc has the newest messages — process it first,
                // then walk history docs newest to oldest.
                CollectFromMessages(_conversation.Messages);

                for (int i = histEnd; i >= histStart && NeedsMore; i--)
                {
                    CollectFromHistoryDoc(_conversation.LinkedConversations[i]);
                }

                if (NeedsMore == false)
                    _hasMoreMessages = true;
            }
        }

        private bool NeedsMore => _results.Count < _pageSize;

        private void CollectFromHistoryDoc(string historyDocId)
        {
            var rawDoc = _storage.Get(_context, historyDocId);
            if (rawDoc?.Data == null)
                return;

            // Intentionally not catching — corrupted history docs should surface a clear error.
            // cloneMessages: false — same borrowing rationale as the main doc: read-only within the open txn.
            var historyConversation = ConversationDocument.ToDocument(historyDocId, rawDoc.Data, maxModelIterationsPerCall: 0, cloneMessages: false);

            if (historyConversation.Messages.Count == 0)
                return;

            CollectFromMessages(historyConversation.Messages);
        }

        // Binary search over LinkedConversations (chronological) to find the range of history docs that may contain relevant messages.
        private (int Start, int End) FindRelevantHistoryRange()
        {
            if (_conversation.LinkedConversations.Count == 0)
                return (0, -1);

            int lo = 0, hi = _conversation.LinkedConversations.Count;

            if (_forward)
            {
                // Find the first doc whose LastMessageAt > _after.
                while (lo < hi)
                {
                    int mid = lo + (hi - lo) / 2;
                    var doc = _storage.Get(_context, _conversation.LinkedConversations[mid]);
                    DateTime lastMessageAt = DateTime.MinValue;
                    doc?.Data?.TryGet(nameof(ConversationDocument.LastMessageAt), out lastMessageAt);

                    if (lastMessageAt <= _after)
                        lo = mid + 1;
                    else
                        hi = mid;
                }

                return (lo, _conversation.LinkedConversations.Count - 1);
            }

            // Find the last doc whose first message < _before.
            while (lo < hi)
            {
                int mid = lo + (hi - lo) / 2;
                var doc = _storage.Get(_context, _conversation.LinkedConversations[mid]);
                DateTime firstMessageAt = DateTime.MinValue;
                if (doc?.Data?.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray messages) == true &&
                    messages is { Length: > 0 })
                {
                    var firstMsg = (BlittableJsonReaderObject)messages[0];
                    firstMsg.TryGet(ConversationDocument.DateProperty, out firstMessageAt);
                }

                if (firstMessageAt < _before)
                    lo = mid + 1;
                else
                    hi = mid;
            }

            return (0, lo - 1);
        }

        private void CollectFromMessages(ConversationDocument.MessagesList messages)
        {
            int startIndex, endIndex;

            if (_forward)
            {
                // > _after to end
                startIndex = BinarySearchBound(messages, _after, inclusive: true);
                endIndex = messages.Count;
            }
            else
            {
                // 0 to <= _before
                startIndex = 0;
                endIndex = BinarySearchBound(messages, _before, inclusive: false);
            }

            if (startIndex >= endIndex)
                return;

            bool stoppedEarly = false;

            if (_forward)
            {
                for (int i = startIndex; i < endIndex; i++)
                {
                    TryProcessMessage(messages, ref i);
                    if (_results.Count >= _pageSize)
                    {
                        stoppedEarly = true; 
                        break;
                    }
                }
            }
            else
            {
                for (int i = endIndex - 1; i >= startIndex; i--)
                {
                    TryProcessMessage(messages, ref i);
                    if (_results.Count >= _pageSize)
                    {
                        stoppedEarly = true; 
                        break;
                    }
                }
            }

            if (stoppedEarly)
                _hasMoreMessages = true;
        }

        private bool TryCollectToolResponse(BlittableJsonReaderObject msg)
        {
            if (IsToolMessage(msg) == false)
                return false;

            msg.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCallId, out string toolCallId);
            if (toolCallId != null)
            {
                msg.TryGet(ChatCompletionClient.Constants.RequestFields.Content, out string content);
                msg.TryGet(ChatCompletionClient.Constants.ResponseFields.SubConversationId, out string subConversationId);
                _toolResponses[toolCallId] = (content, subConversationId);
            }
            return true;
        }

        private static (long TimestampTicks, string Role, string ToolCallId, int Hash) CreateDeduplicationKey(BlittableJsonReaderObject msg)
        {
            // (TimestampTicks, Role, ToolCallId) alone is not enough: ToolCallId is null on user/assistant/system
            // messages, and in old conversations created before the monotonic-timestamp fix several messages can
            // share the same date tick. We add a hash of the serialized message as a strong discriminator —
            // BlittableJsonReaderObject.ToString() produces deterministic JSON, so the same logical message
            // (present in both the current doc and a history snapshot) hashes identically and still dedups,
            // while distinct messages that happen to share (ticks, role) no longer collide.
            msg.TryGet(ConversationDocument.DateProperty, out DateTime timestamp);
            msg.TryGet(ChatCompletionClient.Constants.RequestFields.Role, out string role);
            msg.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCallId, out string toolCallId);
            int hash = msg.ToString().GetHashCode();
            return (timestamp.Ticks, role, toolCallId, hash);
        }

        private void TryProcessMessage(ConversationDocument.MessagesList messages, ref int index)
        {
            var msg = messages[index];

            if (_seenMessageKeys.Add(CreateDeduplicationKey(msg)) == false) // already seen this message - skip
                return;

            if (TryCollectToolResponse(msg))
                return;

            // For forward paging only: tool responses follow their parent assistant message chronologically.
            // Scan ahead to collect them so they're available when ParseAndConvertMessage resolves tool call results.
            // In backward mode, tool responses are encountered before the assistant message, so they're already collected.
            if (_forward &&
                msg.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray toolCalls) &&
                toolCalls is { Length: > 0 })
            {
                for (int j = index + 1; j < messages.Count; j++)
                {
                    if (TryCollectToolResponse(messages[j]) == false)
                        break;
                    index = j; // advance main loop index to skip processed tool response messages
                }
            }

            var converted = ParseAndConvertMessage(msg, _toolResponses);
            if (converted == null)
                return; // internal or unrecognized role

            if (PassesDetailFilter(converted) == false)
                return;

            if (converted.Attachments is { Count: > 0 })
                _attachmentNames.AddRange(converted.Attachments);

            _results.Add(converted);
        }

        private bool PassesDetailFilter(AiConversationMessage msg)
        {
            return _detailLevel switch
            {
                AiConversationDetailLevel.Detailed =>
                    msg.Role is AiMessageRole.System or AiMessageRole.User or AiMessageRole.Assistant,
                AiConversationDetailLevel.Simple => msg switch
                {
                    { Role: AiMessageRole.User } or
                    { Role: AiMessageRole.Assistant, Content: not null } => true,
                    _ => false
                },
                AiConversationDetailLevel.Full or _ => true,
            };
        }

        private static bool IsToolMessage(BlittableJsonReaderObject msg)
        {
            msg.TryGet(ChatCompletionClient.Constants.RequestFields.Role, out string role);
            return role == ChatCompletionClient.Constants.RequestFields.RoleToolValue;
        }

        private static DateTime GetMessageTimestamp(ConversationDocument.MessagesList messages, int index)
        {
            messages[index].TryGet(ConversationDocument.DateProperty, out DateTime date);
            return date;
        }

        private static int BinarySearchBound(ConversationDocument.MessagesList messages, DateTime target, bool inclusive)
        {
            // inclusive = true: skip target
            int lo = 0, hi = messages.Count;
            while (lo < hi)
            {
                int mid = lo + (hi - lo) / 2;
                int cmp = GetMessageTimestamp(messages, mid).CompareTo(target);
                if (cmp < 0 || (inclusive && cmp == 0))
                    lo = mid + 1;
                else
                    hi = mid;
            }
            return lo;
        }

        private static AiConversationMessage ParseAndConvertMessage(BlittableJsonReaderObject msg, Dictionary<string, (string Content, string SubConversationId)> toolResponses)
        {
            msg.TryGet(ChatCompletionClient.Constants.RequestFields.Role, out string role);

            AiMessageRole? apiRole = role switch
            {
                ChatCompletionClient.Constants.RequestFields.RoleSystemValue => AiMessageRole.System,
                ChatCompletionClient.Constants.RequestFields.RoleUserValue => AiMessageRole.User,
                ChatCompletionClient.Constants.RequestFields.RoleAssistantValue
                    when msg.TryGet(ConversationDocument.SummaryProperty, out bool isSummary) && isSummary =>
                        AiMessageRole.Summary,
                ChatCompletionClient.Constants.RequestFields.RoleAssistantValue => AiMessageRole.Assistant,
                ChatCompletionClient.Constants.RequestFields.RoleToolValue => null, // merged into assistant's ToolCalls
                ChatCompletionClient.Constants.RequestFields.RoleInternalValue => AiMessageRole.Internal,
                _ => null
            };
            if (apiRole == null)
                return null;

            string content = null;
            List<string> attachments = null;

            msg.TryGetMember(ChatCompletionClient.Constants.RequestFields.Content, out var contentObj);
            if (contentObj is BlittableJsonReaderArray contentArray)
            {
                var sb = new StringBuilder();
                for (int i = 0; i < contentArray.Length; i++)
                {
                    if (contentArray[i] is not BlittableJsonReaderObject part)
                        continue;

                    part.TryGet("type", out string type);
                    switch (type)
                    {
                        case "text" when part.TryGet("text", out string text):
                            if (sb.Length > 0)
                                sb.Append('\n');
                            sb.Append(text);
                            break;

                        case "image_url" or "image" when part.TryGet("name", out string name) && name != null:
                            (attachments ??= []).Add(name);
                            break;
                    }
                }
                content = sb.Length > 0 ? sb.ToString() : null;
            }
            else
            {
                content = contentObj?.ToString();
            }

            msg.TryGet(ConversationDocument.DateProperty, out DateTime timestamp);

            AiUsage usage = null;
            if (msg.TryGet(ConversationDocument.UsageProperty, out BlittableJsonReaderObject usageObj) && usageObj != null)
                usage = JsonDeserializationClient.AiUsage(usageObj);

            // For internal messages (sub-agent calls), extract the sub-conversation ID
            string subConversationId = null;
            if (apiRole == AiMessageRole.Internal)
                msg.TryGet(ChatCompletionClient.Constants.ResponseFields.SubConversationId, out subConversationId);

            var result = new AiConversationMessage
            {
                Role = apiRole.Value,
                Content = content,
                Attachments = attachments,
                Timestamp = timestamp,
                Usage = usage,
                SubConversationId = subConversationId
            };

            if (msg.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray toolCallsArray) && toolCallsArray != null)
            {
                result.ToolCalls = new List<AiToolCallResult>(toolCallsArray.Length);
                for (int i = 0; i < toolCallsArray.Length; i++)
                {
                    var tcObj = (BlittableJsonReaderObject)toolCallsArray[i];
                    tcObj.TryGet(ChatCompletionClient.Constants.ResponseFields.Id, out string tcId);
                    string tcName = null, tcArgs = null;
                    if (tcObj.TryGet(ChatCompletionClient.Constants.ResponseFields.Function, out BlittableJsonReaderObject func) && func != null)
                    {
                        func.TryGet(ChatCompletionClient.Constants.ResponseFields.Name, out tcName);
                        func.TryGet(ChatCompletionClient.Constants.ResponseFields.Arguments, out tcArgs);
                    }

                    string toolResult = null;
                    string tcSubConversationId = null;
                    if (toolResponses?.TryGetValue(tcId, out var toolResponse) == true)
                    {
                        toolResult = toolResponse.Content;
                        tcSubConversationId = toolResponse.SubConversationId;
                    }

                    result.ToolCalls.Add(new AiToolCallResult
                    {
                        Id = tcId,
                        Name = tcName,
                        Arguments = tcArgs,
                        Result = toolResult,
                        SubConversationId = tcSubConversationId
                    });
                }
            }

            return result;
        }
    }
}
