using System;
using System.Collections.Generic;
using System.Text;
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
        private readonly List<string> _linkedIds;
        private readonly int _pageSize;
        private readonly AiConversationDetailLevel _detailLevel;
        private readonly bool _forward;
        private readonly DateTime _before;
        private readonly DateTime _after;

        private readonly List<AiConversationMessage> _results = new();
        private readonly Dictionary<string, string> _toolResponses = new(StringComparer.Ordinal);
        private readonly HashSet<(long TimestampTicks, string Role, string ToolCallId)> _seenMessageKeys = new();
        private bool _hasMoreMessages;

        public Collector(DocumentsOperationContext context, DocumentsStorage storage, List<string> linkedIds,
            int pageSize, AiConversationDetailLevel detailLevel, DateTime? before, DateTime? after)
        {
            _context = context;
            _storage = storage;
            _linkedIds = linkedIds;
            _pageSize = pageSize;
            _detailLevel = detailLevel;
            _forward = after.HasValue;
            _after = after ?? DateTime.MinValue;
            _before = before ?? DateTime.MaxValue;
        }

        public bool HasMoreMessages => _hasMoreMessages;

        public List<AiConversationMessage> GetResults()
        {
            _results.Sort((a, b) => a.Timestamp.CompareTo(b.Timestamp));
            return _results;
        }

        /// <summary>
        /// Main entry point: collects messages from the current doc and history docs
        /// in the correct order for the paging direction.
        /// </summary>
        public void Collect(List<BlittableJsonReaderObject> currentMessages)
        {
            var (histStart, histEnd) = FindRelevantHistoryRange();

            if (_forward)
            {
                // Trimming moves older messages from the current doc into history, so the
                // chronologically earliest messages matching `after` may live in history docs.
                // Process history first (oldest to newest), then current doc.
                for (int i = histStart; i <= histEnd && NeedsMore; i++)
                    CollectFromHistoryDoc(_linkedIds[i]);

                bool visitedCurrentDoc = false;
                if (NeedsMore)
                {
                    CollectFromMessages(currentMessages);
                    visitedCurrentDoc = true;
                }

                // If we filled the page without reaching the current doc,
                // there are *obviously* newer messages beyond this page.
                _hasMoreMessages |= visitedCurrentDoc == false;
            }
            else
            {
                // For backward paging, current doc has the newest messages — process it first,
                // then walk history docs newest to oldest.
                CollectFromMessages(currentMessages);

                if (NeedsMore == false && histStart <= histEnd)
                {
                    // Page was filled from current doc alone, but history docs with older
                    // messages exist — let the client know there's more to page into.
                    _hasMoreMessages = true;
                }
                else
                {
                    for (int i = histEnd; i >= histStart && NeedsMore; i--)
                        CollectFromHistoryDoc(_linkedIds[i]);
                }
            }
        }

        private bool NeedsMore => _results.Count < _pageSize;

        private void CollectFromHistoryDoc(string historyDocId)
        {
            var rawDoc = _storage.Get(_context, historyDocId);
            if (rawDoc?.Data == null)
                return;

            ConversationDocument historyConversation = ConversationDocument.ToDocument(historyDocId, rawDoc.Data, maxModelIterationsPerCall: 0);

            if (historyConversation.Messages.Count == 0)
                return;

            CollectFromMessages(historyConversation.Messages);
        }

        /// <summary>
        /// Returns the (start, end) inclusive range of LinkedConversation indices that may
        /// contain messages relevant to the current paging direction and timestamp bounds.
        /// Binary search — LinkedConversations is chronologically ordered.
        /// Forward: finds first doc whose LastMessageAt &gt; _after through to the end.
        /// Backward: finds all docs whose first message &lt; _before.
        ///
        /// Note: we intentionally access the raw blittable here rather than using
        /// ConversationDocument.ToDocument(), because we only need to peek at a single
        /// timestamp field per doc. ToDocument() would clone all messages and parse
        /// every field — wasteful during a binary search that may skip most docs.
        /// </summary>
        private (int Start, int End) FindRelevantHistoryRange()
        {
            if (_linkedIds.Count == 0)
                return (0, -1);

            int lo = 0, hi = _linkedIds.Count;
            while (lo < hi)
            {
                int mid = lo + (hi - lo) / 2;
                var doc = _storage.Get(_context, _linkedIds[mid]);

                bool goLow;
                if (_forward)
                {
                    DateTime lastMessageAt = DateTime.MinValue;
                    doc?.Data?.TryGet(nameof(ConversationDocument.LastMessageAt), out lastMessageAt);
                    goLow = lastMessageAt <= _after;
                }
                else
                {
                    DateTime firstMessageAt = DateTime.MaxValue;
                    if (doc?.Data?.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray messages) == true &&
                        messages is { Length: > 0 })
                    {
                        var firstMsg = (BlittableJsonReaderObject)messages[0];
                        firstMsg.TryGet(ConversationDocument.DateProperty, out firstMessageAt);
                    }
                    goLow = firstMessageAt < _before;
                }

                if (goLow)
                    lo = mid + 1;
                else
                    hi = mid;
            }

            return _forward ? (lo, _linkedIds.Count - 1) : (0, lo - 1);
        }

        private void CollectFromMessages(List<BlittableJsonReaderObject> messages)
        {
            int startIndex, endIndex;

            if (_forward)
            {
                startIndex = BinarySearchBound(messages, _after, inclusive: true);
                endIndex = messages.Count;
            }
            else
            {
                startIndex = 0;
                endIndex = BinarySearchBound(messages, _before, inclusive: false);
            }

            if (startIndex >= endIndex)
                return;

            int start = _forward ? startIndex : endIndex - 1;
            int step = _forward ? 1 : -1;
            bool stoppedEarly = false;

            for (int i = start; i >= startIndex && i < endIndex; i += step)
            {
                TryProcessMessage(messages, ref i);

                if (_results.Count >= _pageSize)
                {
                    stoppedEarly = true;
                    break;
                }
            }

            if (stoppedEarly)
                _hasMoreMessages = true;
            else if (_forward == false)
                _hasMoreMessages |= startIndex > 0; // older messages exist before our range in this doc
        }

        private bool TryCollectToolResponse(BlittableJsonReaderObject msg)
        {
            if (IsToolMessage(msg) == false)
                return false;

            msg.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCallId, out string toolCallId);
            if (toolCallId != null)
            {
                msg.TryGet(ChatCompletionClient.Constants.RequestFields.Content, out string content);
                _toolResponses[toolCallId] = content;
            }
            return true;
        }

        private static (long TimestampTicks, string Role, string ToolCallId) CreateDeduplicationKey(BlittableJsonReaderObject msg)
        {
            msg.TryGet(ConversationDocument.DateProperty, out DateTime timestamp);
            msg.TryGet(ChatCompletionClient.Constants.RequestFields.Role, out string role);
            msg.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCallId, out string toolCallId);
            return (timestamp.Ticks, role, toolCallId);
        }

        private void TryProcessMessage(List<BlittableJsonReaderObject> messages, ref int index)
        {
            var msg = messages[index];

            if (_seenMessageKeys.Add(CreateDeduplicationKey(msg)) == false)
                return;

            if (TryCollectToolResponse(msg))
                return;

            // For forward paging, tool responses follow this message. Scan ahead to collect
            // them so they're available when ParseAndConvertMessage resolves tool call results.
            if (msg.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray toolCalls) &&
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

        private static DateTime GetMessageTimestamp(List<BlittableJsonReaderObject> messages, int index)
        {
            messages[index].TryGet(ConversationDocument.DateProperty, out DateTime date);
            return date;
        }

        private static int BinarySearchBound(List<BlittableJsonReaderObject> messages, DateTime target, bool inclusive)
        {
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

        private static AiConversationMessage ParseAndConvertMessage(BlittableJsonReaderObject msg, Dictionary<string, string> toolResponses)
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

            var result = new AiConversationMessage
            {
                Role = apiRole.Value,
                Content = content,
                Attachments = attachments,
                Timestamp = timestamp,
                Usage = usage
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
                    toolResponses?.TryGetValue(tcId, out toolResult);

                    result.ToolCalls.Add(new AiToolCallResult
                    {
                        Id = tcId,
                        Name = tcName,
                        Arguments = tcArgs,
                        Result = toolResult
                    });
                }
            }

            return result;
        }
    }
}
