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
        private readonly HashSet<DateTime> _seenTimestamps = new();
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
        public bool NeedsMore => _results.Count < _pageSize;

        public List<AiConversationMessage> GetResults()
        {
            _results.Sort((a, b) => a.Timestamp.CompareTo(b.Timestamp));
            return _results;
        }

        public void CollectFromDocument(BlittableJsonReaderArray messages, DateTime? before, DateTime? after)
        {
            if (messages == null || messages.Length == 0)
                return;

            CollectFromMessages(messages, before, after);

            if (_linkedIds.Count > 0)
                _hasOlderMessages = true;
        }

        public void CollectFromHistoryDoc(string historyDocId, DateTime? before, DateTime? after)
        {
            var historyDoc = _storage.Get(_context, historyDocId);

            if (historyDoc == null)
                return;

            if (historyDoc.Data.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray historyMessages) == false ||
                historyMessages == null || historyMessages.Length == 0)
                return;

            CollectFromMessages(historyMessages, before, after);
        }

        private void CollectFromMessages(BlittableJsonReaderArray messages, DateTime? before, DateTime? after)
        {
            int startIndex, endIndex;

            if (after.HasValue)
            {
                startIndex = BinarySearchBound(messages, after.Value, inclusive: true);
                endIndex = messages.Length;
            }
            else if (before.HasValue)
            {
                startIndex = 0;
                endIndex = BinarySearchBound(messages, before.Value, inclusive: false);
            }
            else
            {
                startIndex = 0;
                endIndex = messages.Length;
            }

            if (startIndex >= endIndex)
                return;

            if (after.HasValue)
            {
                // Forward: collect from start, keep scanning tool responses past pageSize
                for (int i = startIndex; i < endIndex; i++)
                {
                    if (_results.Count >= _pageSize && IsToolMessage(messages, i) == false)
                        break;

                    TryProcessMessage(messages, i);
                }
            }
            else
            {
                // Backward/default: tool responses come after their assistant message chronologically,
                // so walking backward we encounter them first — they're in _toolResponses by the time
                // we hit the assistant message.
                int lastVisited = endIndex;
                for (int i = endIndex - 1; i >= startIndex; i--)
                {
                    TryProcessMessage(messages, i);
                    lastVisited = i;

                    if (_results.Count >= _pageSize)
                        break;
                }

                if (lastVisited > startIndex)
                    _hasOlderMessages = true;
            }
        }

        private void TryProcessMessage(BlittableJsonReaderArray messages, int index)
        {
            var msg = (BlittableJsonReaderObject)messages[index];
            msg.TryGet(ConversationDocument.DateProperty, out DateTime timestamp);

            if (_seenTimestamps.Add(timestamp) == false)
                return;

            msg.TryGet(ChatCompletionClient.Constants.RequestFields.Role, out string role);

            // Tool responses: accumulate for later merging, don't count toward page
            if (role == ChatCompletionClient.Constants.RequestFields.RoleToolValue)
            {
                msg.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCallId, out string toolCallId);
                if (toolCallId != null)
                {
                    msg.TryGet(ChatCompletionClient.Constants.RequestFields.Content, out string content);
                    _toolResponses[toolCallId] = content;
                }
                return;
            }

            var converted = ParseAndConvertMessage(msg, _toolResponses);
            if (converted == null)
                return;

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

        private static bool IsToolMessage(BlittableJsonReaderArray messages, int index)
        {
            var msg = (BlittableJsonReaderObject)messages[index];
            msg.TryGet(ChatCompletionClient.Constants.RequestFields.Role, out string role);
            return role == ChatCompletionClient.Constants.RequestFields.RoleToolValue;
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
                            sb.AppendLine(text);
                            break;

                        case "image_url" or "image" when part.TryGet("name", out string name) && name != null:
                            (attachments ??= []).Add(name);
                            break;
                    }
                }
                content = sb.Length > 0 ? sb.ToString().TrimEnd() : null;
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
