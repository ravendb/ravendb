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
    /// Collects, groups, and filters messages in a single pass per document.
    /// Tool responses are accumulated into a dictionary and merged into their
    /// parent assistant message's ToolCalls during parsing.
    /// For backward paging, tool responses are naturally encountered before
    /// their parent assistant message. For forward paging, we keep scanning
    /// past pageSize to collect tool responses (they don't count toward the page).
    /// </summary>
    private sealed class Collector
    {
        private readonly DocumentsOperationContext _context;
        private readonly DocumentsStorage _storage;
        private readonly List<string> _linkedIds;
        private readonly int _pageSize;
        private readonly AiConversationDetailLevel _detailLevel;

        private readonly List<AiConversationMessage> _results = new();
        private readonly Dictionary<string, string> _toolResponses = new(StringComparer.Ordinal);
        private readonly HashSet<string> _seenMessageKeys = new(StringComparer.Ordinal);
        private bool _hasOlderMessages;

        public Collector(DocumentsOperationContext context, DocumentsStorage storage, List<string> linkedIds,
            int pageSize, AiConversationDetailLevel detailLevel)
        {
            _context = context;
            _storage = storage;
            _linkedIds = linkedIds;
            _pageSize = pageSize;
            _detailLevel = detailLevel;
        }

        public bool HasOlderMessages => _hasOlderMessages;

        public List<AiConversationMessage> GetResults()
        {
            _results.Sort((a, b) => a.Timestamp.CompareTo(b.Timestamp));
            return _results;
        }

        /// <summary>
        /// Main entry point: collects messages from the current doc and history docs
        /// in the correct order for the paging direction.
        /// </summary>
        public void Collect(BlittableJsonReaderArray currentMessages, DateTime? before, DateTime? after)
        {
            if (after is not null)
            {
                // User is asking for messages after T, page size 5. 
                // T1 .. T3 were moved to history, T4 .. T6 are in the current doc. 
                // We want to return T1..T5, but need to process history first
                int startDoc = FindHistoryDocBound(after.Value, forward: true);

                // For `after` paging, history docs must be processed BEFORE the current doc.
                for (int i = startDoc; i < _linkedIds.Count && NeedsMore; i++)
                    CollectFromHistoryDoc(_linkedIds[i], before, after);

                if (NeedsMore)
                    CollectFromMessages(currentMessages, before, after);
                return;
            }

            // For backward paging, the current doc has the newest messages — process it first.
            CollectFromMessages(currentMessages, before, after);

            if (NeedsMore)
            {
                int endDoc = before is not null
                    ? FindHistoryDocBound(before.Value, forward: false)
                    : _linkedIds.Count - 1;
                for (int i = endDoc; i >= 0 && NeedsMore; i--)
                    CollectFromHistoryDoc(_linkedIds[i], before, after);
            }
        }

        private bool NeedsMore => _results.Count < _pageSize;

        private void CollectFromHistoryDoc(string historyDocId, DateTime? before, DateTime? after)
        {
            var historyDoc = _storage.Get(_context, historyDocId);

            if (historyDoc == null)
                return;

            if (historyDoc.Data.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray historyMessages) == false ||
                historyMessages == null || historyMessages.Length == 0)
                return;

            CollectFromMessages(historyMessages, before, after);
        }

        /// <summary>
        /// Binary search LinkedConversations (chronologically ordered) to find the relevant
        /// starting doc for paging.
        /// When forward=true: returns the first doc that may contain messages after the timestamp.
        /// When forward=false: returns the last doc that may contain messages before the timestamp.
        /// </summary>
        private int FindHistoryDocBound(DateTime target, bool forward)
        {
            int lo = 0, hi = _linkedIds.Count;
            while (lo < hi)
            {
                int mid = lo + (hi - lo) / 2;

                var doc = _storage.Get(_context, _linkedIds[mid]);
                bool goLow;
                if (forward)
                {
                    DateTime lastMessageAt = DateTime.MinValue;
                    doc?.Data?.TryGet(nameof(ConversationDocument.LastMessageAt), out lastMessageAt);
                    goLow = lastMessageAt <= target;
                }
                else
                {
                    DateTime firstMessageAt = DateTime.MaxValue;
                    if (doc?.Data?.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray messages) == true &&
                        messages is { Length: > 0 })
                        firstMessageAt = GetMessageTimestamp(messages, 0);
                    goLow = firstMessageAt < target;
                }

                if (goLow)
                    lo = mid + 1;
                else
                    hi = mid;
            }

            return forward ? lo : lo - 1;
        }

        private void CollectFromMessages(BlittableJsonReaderArray messages, DateTime? before, DateTime? after)
        {
            int startIndex, endIndex;
            bool forward;

            if (after.HasValue)
            {
                startIndex = BinarySearchBound(messages, after.Value, inclusive: true);
                endIndex = messages.Length;
                forward = true;
            }
            else if (before.HasValue)
            {
                startIndex = 0;
                endIndex = BinarySearchBound(messages, before.Value, inclusive: false);
                forward = false;
            }
            else
            {
                startIndex = 0;
                endIndex = messages.Length;
                forward = false;
            }

            if (startIndex >= endIndex)
                return;


            int start = forward ? startIndex : endIndex - 1;
            int step = forward ? 1 : -1;
            bool stoppedEarly = false;

            for (int i = start; i >= startIndex && i < endIndex; i += step)
            {
                TryProcessMessage(messages, i);

                if (_results.Count >= _pageSize)
                {
                    stoppedEarly = true;
                    break;
                }
            }

            if (stoppedEarly || startIndex > 0)
                _hasOlderMessages = true;
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

        private static string CreateDeduplicationKey(BlittableJsonReaderObject msg)
        {
            msg.TryGet(ConversationDocument.DateProperty, out DateTime timestamp);
            msg.TryGet(ChatCompletionClient.Constants.RequestFields.Role, out string role);
            msg.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCallId, out string toolCallId);
            return $"{timestamp.Ticks}|{role}|{toolCallId}";
        }

        private void TryProcessMessage(BlittableJsonReaderArray messages, int index)
        {
            var msg = (BlittableJsonReaderObject)messages[index];

            if (_seenMessageKeys.Add(CreateDeduplicationKey(msg)) == false)
                return;

            if (TryCollectToolResponse(msg))
                return;

            // For forward paging, tool responses follow this message. Scan ahead to collect
            // them so they're available when ParseAndConvertMessage resolves tool call results.
            if (msg.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray toolCalls) &&
                toolCalls is { Length: > 0 })
            {
                for (int j = index + 1; j < messages.Length; j++)
                {
                    var next = (BlittableJsonReaderObject)messages[j];
                    if (TryCollectToolResponse(next) == false)
                        break; // hit a non-tool message, stop scanning
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

        private static DateTime GetMessageTimestamp(BlittableJsonReaderArray messages, int index)
        {
            var msg = (BlittableJsonReaderObject)messages[index];
            msg.TryGet(ConversationDocument.DateProperty, out DateTime date);
            return date;
        }

        private static int BinarySearchBound(BlittableJsonReaderArray messages, DateTime target, bool inclusive)
        {
            int lo = 0, hi = messages.Length;
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
