using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Newtonsoft.Json;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.Handlers.Processors.MultiGet;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using GetRequest = Raven.Client.Documents.Commands.MultiGet.GetRequest;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public partial class ConversationHandler(ServerStore server, DocumentDatabase database)
{
    internal static Action<string, AiUsage> OnUpdateUsage;

    public const int DefaultMaxModelIterationsPerCall = 16;
    private const int DefaultMaxTokensBeforeSummarization = 32 * 1024;
    private const int DefaultMaxTokensAfterSummarization = 1024;
    private const string QueryVirtualSubConversationId = "_QueryTools_";

    protected ConversationDocument _document;
    protected string _conversationId;
    protected RequestBody _request;
    private AiAgentConfiguration _configuration;
    private string _changeVector;
    private string _raftId;
    private bool? _debugOverride;
    private bool _cancelPendingActionTools;
    protected int _maxModelIterationsPerCall;
    internal List<string> _persistedAttachmentsNames;
    public required RavenServer.AuthenticateConnection Authentication;
    public void Initialize(AiAgentConfiguration configuration, string conversationId, RequestBody body, string changeVector, string raftId = null, bool? debugOverride = null, bool cancelPendingActionTools = false)
    {
        _conversationId = conversationId;
        _request = body;
        _configuration = configuration;
        _changeVector = changeVector;
        _raftId = raftId;
        _debugOverride = debugOverride;
        _maxModelIterationsPerCall = GetMaxModelIterationsPerCall(body, configuration);
        _cancelPendingActionTools = cancelPendingActionTools;
    }

    protected virtual async Task InitializeDocumentAsync(DocumentsOperationContext context, CancellationToken token)
    {
        var agentId = _configuration.Identifier;

        using var __ = context.OpenReadTransaction();
        var conversation = database.DocumentsStorage.Get(context, _conversationId);
        if (conversation == null)
        {
            if (string.IsNullOrEmpty(_changeVector) == false)
            {
                throw new ConcurrencyException(
                    $"The conversation '{_conversationId}' doesn't exists.")
                {
                    ExpectedChangeVector = _changeVector,
                    ActualChangeVector = string.Empty,
                    Id = _conversationId
                };
            }

            if (RequestBody.HasUserPrompt(_request.Content) == false && _request.Attachments is { Count: > 0 } == false && _request.AttachmentCommands?.ParsedCommands is {Count: > 0} == false)
            {
                throw new InvalidOperationException(
                    $"Cannot start a new conversation '{_conversationId}' without a user prompt.");
            }

            ValidateParameterValues(_request.Parameters);
            _document = new ConversationDocument(agentId, _request.Parameters);
            _document.Id = await GetDocumentIdAsync();

            if (_request.CreationOptions.ExpirationInSec.HasValue)
            {
                _document.Expires = TimeSpan.FromSeconds(_request.CreationOptions.ExpirationInSec.Value);
            }

            _document.Initialize(context, _configuration, resetRemainingToolIterations: true, _maxModelIterationsPerCall);
            if (_document.InitialOperations(context, _configuration) is { } queries)
            {
                // run initial tool calls...
                await HandleQueryAndAgentCallsAsync(context, queries, token);
            }
        }
        else
        {
            _document = ConversationDocument.ToDocument(_conversationId, conversation.Data, _maxModelIterationsPerCall);
            if (_document.Agent != agentId)
            {
                throw new InvalidOperationException(
                    $"The conversation '{_conversationId}' is assigned to agent '{_document.Agent}', " +
                    $"but the request is for agent '{agentId}'.");
            }

            if (_changeVector != null)
            {
                if (conversation.ChangeVector != _changeVector)
                    throw new ConcurrencyException(
                        $"The conversation '{_conversationId}' was updated and doesn't match the expected change vector. Reload the conversation and try again.")
                    {
                        ExpectedChangeVector = _changeVector,
                        ActualChangeVector = conversation.ChangeVector,
                        Id = _conversationId
                    };

                _document.ChangeVector = conversation.ChangeVector;
            }
        }

        if (_debugOverride.HasValue)
            _document.Debug = _debugOverride.Value;

        if (_request.AttachmentCommands?.ParsedCommands is { Count: >0})
        {
            using var it = _request.AttachmentCommands.AttachmentStreams?.GetEnumerator() ?? default;
            foreach (var cmd in _request.AttachmentCommands.ParsedCommands)
            {
                switch (cmd.Type)
                {
                    case CommandType.AttachmentCOPY:
                        cmd.DestinationId = _document.Id;
                        ConversationHandlerAttachments.RetrieveAndAddAttachment(database, context, _request, _conversationId, cmd.Name, cmd.Id);
                        break;
                    case CommandType.AttachmentPUT:
                        cmd.Id = _document.Id;
                        if (it.MoveNext() == false) 
                            throw new InvalidOperationException($"Missing attachment stream for '{cmd.Name}' in conversation '{_conversationId}'.");

                        it.Current.Stream.Position =0;

                        ConversationHandlerAttachments.AddPutAttachmentFromStream(_request, it.Current.Stream, cmd.Name, cmd.ContentType);
                        break;

                    default:
                        throw new NotSupportedException($"{cmd.Type} is not supported");
                }
            }
        }
        _persistedAttachmentsNames = ConversationHandlerAttachments.GetConversationPersistedAttachmentsNames(database, context, _document.Id);
    }

    private void ValidateParameterValues(BlittableJsonReaderObject requestParameters)
    {
        // Validate that the provided parameter values match the expected types in the configuration.

        if (_configuration.Parameters == null || requestParameters == null)
            return;

        foreach (var configParam in _configuration.Parameters)
        {
            if (requestParameters.TryGetMember(configParam.Name, out object value) == false)
                continue;

            value = GetAiConversationParameter(configParam.Name, value).Value;
            var expectedType = configParam.Type;

            if (expectedType == AiAgentParameterValueType.Default)
                continue;

            if (value is BlittableJsonReaderArray { Length: 0 })
            {
                if (expectedType is not (AiAgentParameterValueType.ArrayOfString or 
                                            AiAgentParameterValueType.ArrayOfBoolean or 
                                            AiAgentParameterValueType.ArrayOfNumber))
                    throw new InvalidCastException(
                        $"Parameter '{configParam.Name}' has invalid type. " +
                        $"Expected: {expectedType}, " +
                        $"Actual: Array (Empty), " +
                        $"Value: {value}");

                continue;
            }

            if (GetValueType(value, out var actualType, out var unsupportedType) == false)
                throw new InvalidCastException(
                    $"Parameter '{configParam.Name}' has unsupported type. " +
                    $"Actual: {unsupportedType}");

            if (actualType != expectedType)
                throw new InvalidCastException(
                    $"Parameter '{configParam.Name}' has invalid type. " +
                    $"Expected: {expectedType}, " +
                    $"Actual: {actualType}, " +
                    $"Value: {value}");
        }
    }

    private static bool GetValueType(object value, out AiAgentParameterValueType type, out string unsupportedType)
    {
        type = default;
        unsupportedType = null;

        switch (value)
        {
            case null:
                type = AiAgentParameterValueType.Null;
                return true;
            case string:
            case LazyStringValue:
            case LazyCompressedStringValue:
                type = AiAgentParameterValueType.String;
                return true;

            case int:
            case long:
            case float:
            case double:
            case decimal:
            case LazyNumberValue:
            case byte:
            case short:
            case sbyte:
            case ushort:
            case uint:
            case ulong:
                type = AiAgentParameterValueType.Number;
                return true;

            case bool:
                type = AiAgentParameterValueType.Boolean;
                return true;

            case BlittableJsonReaderArray array:
                bool first = true;
                var elementType = AiAgentParameterValueType.Default;

                // make sure all elements have the same type
                // make sure all elements have a valid type
                foreach (var element in array)
                {
                    if (GetValueType(element, out var curType, out var elementUnsupportedType) == false)
                    {
                        unsupportedType = $"Array contains an element of unsupported type '{elementUnsupportedType}'.";
                        return false;
                    }

                    if (curType is AiAgentParameterValueType.ArrayOfBoolean or AiAgentParameterValueType.ArrayOfNumber or AiAgentParameterValueType.ArrayOfString)
                    {
                        unsupportedType = "Array of arrays.";
                        return false;
                    }

                    if (first)
                    {
                        first = false;
                        elementType = curType;
                        continue;
                    }

                    if (elementType != curType)
                    {
                        unsupportedType = $"Array of mixed element types: '{elementType}' and '{curType}'.";
                        return false;
                    }
                }

                type = elementType switch
                {
                    AiAgentParameterValueType.String => AiAgentParameterValueType.ArrayOfString,
                    AiAgentParameterValueType.Number => AiAgentParameterValueType.ArrayOfNumber,
                    AiAgentParameterValueType.Boolean => AiAgentParameterValueType.ArrayOfBoolean,
                    _ => default
                };

                if (type == default)
                {
                    unsupportedType = $"Array of unsupported element type '{elementType}'.";
                    return false;
                }

                return true;
            case BlittableJsonReaderObject:
                unsupportedType = "Object";
                return false;
            default:
                unsupportedType = value.GetType().Name;
                return false;
        }
    }

    public static AiConversationParameter GetAiConversationParameter(string paramName, object paramValue)
    {
        var sendToModel = true; // At the conversation level
        var realValue = paramValue;
        if (paramValue is BlittableJsonReaderObject obj)
        {
            // Backward compatibility:
            // * Old payload format:
            // {
            //     "maxBudgetNis": 3500
            // }
            // * New payload format:
            // {
            //     "maxBudgetNis": {
            //         "value": 3500,
            //         "sendToModel": true
            //     }
            // }
            // If the parameter is not an object with a "value" field, we assume it's the old format and use the value directly.

            if (obj.TryGetMember(nameof(AiConversationParameter.Value), out realValue) == false)
            {
                // Should never reach here - Object parameters are not supported
                throw new InvalidCastException(
                    $"Parameter '{paramName}' has unsupported type. " +
                    $"Actual: Object");
            }
            if (obj.TryGet(nameof(AiConversationParameter.SendToModel), out sendToModel) == false)
                sendToModel = true; //default
        }

        return new AiConversationParameter
        {
            Value = realValue,
            SendToModel = sendToModel
        };
    }



    private ChatCompletionClient _client;

    public void SetClient([NotNull] ChatCompletionClient client) => _client = client;

    protected internal virtual ChatCompletionClient CreateClient()
    {
        if (_client != null)
            return _client;

        var connection = GetAiConnectionString();
        return _client = ChatCompletionClient.CreateChatCompletionClient(database.DocumentsStorage.ContextPool, connection);
    }

    public static int GetMaxModelIterationsPerCall(RequestBody body, AiAgentConfiguration configuration)
        => body.CreationOptions.MaxModelIterationsPerCall ?? configuration.MaxModelIterationsPerCall ?? DefaultMaxModelIterationsPerCall;

    public async Task<AiInternalConversationResult> StreamingTalkAsync(
        JsonOperationContext context,
        string firstStreamPropertyPath,
        Func<Memory<byte>, Task> streaming,
        CancellationToken token)
    {
        using var talker = new Talker(this, context, _configuration, _document, firstStreamPropertyPath, streaming);
        return await RunInternalAsync(context, talker, token);
    }
        
    public async Task<AiInternalConversationResult> TalkAsync(
        JsonOperationContext context,
        CancellationToken token)
    {
        using var talker = new Talker(this, context, _configuration, _document, firstStreamPropertyPath: null, streaming: null);
        return await RunInternalAsync(context, talker, token);
    }

    private TimeSpan _elapsed;

    private async Task<AiInternalConversationResult> RunInternalAsync(
        JsonOperationContext context,
        Talker talker, CancellationToken token)
    {
        talker.Init();
        var toolsIterations = 0;

        // Resolve deferred attachments before talking to the model
        await ResolveDeferredAttachmentsAsync(_request.Attachments, token);

        AiResponse r = default;
        List<BlittableJsonReaderObject> historyDocs = default;
        bool shouldContinueConversation = true;
        var sw = Stopwatch.StartNew();

        var pendingAlertsDetails = new List<ExceededTokenThresholdDetails>();
        bool isFirstIteration = true;
        var debugTraces = new AiDebugTraceCollector(_document.Debug, database);

        try
        {
            while (shouldContinueConversation)
            {
                token.ThrowIfCancellationRequested();

                var attachments = _request.Attachments ?? new List<AiAttachment>();

                database.ForTestingPurposes?.BeforeAiAgentTalk?.Invoke(talker.Document);

                var trace = debugTraces.CreateTrace();

                using var request = talker.CreateCompletionRequest(attachments, trace);
                r = await talker.RunAsync(database.DocumentsStorage.ContextPool, request, trace, token);

                var currentTurnUsage = AiUsage.GetUsageDifference(talker.AiUsage, _document.CurrentUsage);
                //we want that message only on when we upload an attachment and not when the internal tool is called.
                if (isFirstIteration && _request.AttachmentCommands?.ParsedCommands.Count > 0)
                {
                    AddMessageWithAttachmentsName(context, isFirstIteration);
                }
                isFirstIteration = false;

                _document.AddMessage(context, r.Message, currentTurnUsage);
                _document.UpdateUsage(talker.AiUsage);
                OnUpdateUsage?.Invoke(database.Name, currentTurnUsage);

                if (currentTurnUsage.PromptTokens > database.Configuration.Ai.ToolsTokenUsageThreshold)
                {
                    if (_document.TryGetDetailsOfRecentToolCall(_configuration, out var toolCalls))
                    {
                        pendingAlertsDetails.Add(ExceededTokenThresholdDetails.Add(
                                database.NotificationCenter,
                                _configuration.Name,
                                _conversationId,
                                currentTurnUsage.PromptTokens,
                                database.Configuration.Ai.ToolsTokenUsageThreshold,
                                toolCalls
                            )
                        );
                    }
                }

                toolsIterations++;
                if (r.Type is AiResponseType.Result)
                {
                    _document.RemainingToolIterations = _maxModelIterationsPerCall;
                    shouldContinueConversation = false;
                }
                else
                {
                    var iterations = await HandleQueryAndAgentCallsAsync(context, r.ToolCalls, token);
                    toolsIterations += iterations;
                    if (TryGetUserTools(context, r.ToolCalls))
                    {
                        shouldContinueConversation = false;
                    }
                    else
                    {
                        //should close the tool calls that were handled internally
                        HandleInternalSystemActionsAsync(context, _document.OpenActionCalls.Values.ToList(), token);
                    }
                }

                _document.CurrentUsage = talker.AiUsage;

                // check if we should summarize or truncate the chat history
                var reductionResult = await TryReduceChatSizeAsync(context, talker.Client, talker.AiUsage, token);
                if (reductionResult != null)
                {
                    historyDocs ??= [];
                    historyDocs.Add(reductionResult);
                }
            }

            _elapsed = sw.Elapsed;
            _conversationId = await TryPersistAsync(context, historyDocs);
        }
        finally
        {
            await debugTraces.PersistAsync(_document);
        }

        foreach (ExceededTokenThresholdDetails pendingAlertDetails in pendingAlertsDetails)
        {
            pendingAlertDetails.ConversationId = _conversationId;
            database.NotificationCenter.Add(
                ExceededTokenThresholdDetails.CreateAlert(pendingAlertDetails, database.Name));
        }

        return new AiInternalConversationResult
        {
            Response = r.Result,
            Usage = talker.AiUsage,
            ToolsIterations = toolsIterations,
        };
    }

    private void AddMessageWithAttachmentsName(JsonOperationContext context, bool isFirstIteration)
    {
        var attachmentNames = string.Join(", ", _request.AttachmentCommands.ParsedCommands.Select(c => c.Name));
        _document.AddMessage(context, context.ReadObject(new DynamicJsonValue
        {
            [ChatCompletionClient.Constants.ResponseFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleUserValue,
            [ChatCompletionClient.Constants.ResponseFields.Content] = $"[Attachments: {attachmentNames}]"
        }, "user/attachments-msg"), usage: null); // usage: null
    }

    private void HandleInternalSystemActionsAsync(JsonOperationContext context, List<AiAgentActionRequest> toolCalls, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();

        if (ConversationHandlerAttachments.NeedsReadTransactionForInternalActions(toolCalls) == false)
            return;

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext docContext))
        using (docContext.OpenReadTransaction())
        {
            ConversationHandlerAttachments.HandleInternalSystemActions(database, context, docContext, _document, _request, _conversationId, toolCalls);
        }
    }

    private async Task<BlittableJsonReaderObject> TryReduceChatSizeAsync(JsonOperationContext context, ChatCompletionClient client, AiUsage aiUsage, CancellationToken token)
    {
        var reduction = _configuration.ChatTrimming;
        if (reduction == null || _document.OpenActionCalls.Count > 0)
            return null;

        // do not use reduction if attachments are being added in the current request.
        // Attachments cause single-turn spike in token usage.
        // which usually trigger chat reduction that is not needed
        // as the raw file data is not persisted on the messages we send to the LLM.
        if (_request.AttachmentCommands?.ParsedCommands.Count > 0)
            return null;

        TimeSpan? historyExpiration = reduction.History?.HistoryExpirationInSec == null
            ? null
            : TimeSpan.FromSeconds(reduction.History.HistoryExpirationInSec.Value);

        if (reduction.Truncate != null)
        {
            if (_document.Messages.Count > reduction.Truncate.MessagesLengthBeforeTruncate)
            {
                var truncateCount = _document.Messages.Count - reduction.Truncate.MessagesLengthAfterTruncate;
                truncateCount = int.Min(truncateCount, _document.Messages.Count - 1); // prevent System.ArgumentException (out of bounds)

                // Avoid splitting a tool call group (assistant with tool_calls + subsequent tool responses).
                // If the cut point lands inside a group, advance past the tool responses to keep the group together.
                int cutIndex = 1 + truncateCount; // first message to keep (0 is system prompt)
                while (cutIndex < _document.Messages.Count)
                {
                    var msg = _document.Messages[cutIndex];
                    if (msg.TryGet(ChatCompletionClient.Constants.RequestFields.Role, out string role) &&
                        role == ChatCompletionClient.Constants.RequestFields.RoleToolValue)
                    {
                        cutIndex++;
                        truncateCount++;
                    }
                    else
                        break;
                }

                truncateCount = int.Min(truncateCount, _document.Messages.Count - 1);
                if (truncateCount > 0)
                {
                    var chatBefore = reduction.History == null ? null : _document.ToHistoryBlittable(context, _configuration, historyExpiration);
                    TrimMessages(_document.Messages, truncateCount);
                    return chatBefore;
                }
            }
        }
        else if (reduction.Tokens != null)
        {
            reduction.Tokens.MaxTokensBeforeSummarization = _configuration.ChatTrimming.Tokens.MaxTokensBeforeSummarization ??
                                                            DefaultMaxTokensBeforeSummarization;
            reduction.Tokens.MaxTokensAfterSummarization = _configuration.ChatTrimming.Tokens.MaxTokensAfterSummarization ??
                                                           DefaultMaxTokensAfterSummarization;

            if (aiUsage.TotalTokens > reduction.Tokens.MaxTokensBeforeSummarization)
            {
                var chatBefore = reduction.History == null ? null : _document.ToHistoryBlittable(context, _configuration, historyExpiration);
                await SummarizeAsync(context, client, _configuration, _document, token);
                return chatBefore;
            }
        }

        return null; // if reduction wasn't executed -> no history to persist (return null)
    }

    /// <summary>
    /// Removes up to <paramref name="truncateCount"/> messages from the beginning of the conversation in place,
    /// while always preserving the system prompt at index 0.
    ///
    /// If an assistant message containing <c>tool_calls</c> is removed, all matching
    /// <c>role = "tool"</c> result messages are also removed to keep the conversation valid.
    ///
    /// This cleanup is required because OpenAI chat APIs reject orphaned tool messages:
    /// a <c>role = "tool"</c> message must always have a corresponding preceding assistant
    /// message containing the matching <c>tool_calls</c> entry.
    /// </summary>
    private static void TrimMessages(
        ConversationDocument.MessagesList messages,
        int truncateCount)
    {
        if (messages == null || messages.Count <= 1 || truncateCount <= 0)
            return;

        // Never remove the system prompt at index 0
        if (truncateCount >= messages.Count - 1)
        {
            messages.RemoveRange(1, messages.Count - 1);
            return;
        }

        var removedCount = 0;
        var toolCallIds = new HashSet<string>();

        int i;
        // Bound by messages.Count (re-evaluated each iteration) so the access below is always safe
        // as the list shrinks. The break condition stops us once we've scheduled enough removals;
        // we don't bound by n+1 because that's the *original* target and the list is shrinking.
        for (i = 1; i < messages.Count; i++)
        {
            if (removedCount + toolCallIds.Count >= truncateCount)
                break;

            var message = messages[i];

            // Message may already be removed because it was deleted as part of tool cleanup
            messages.RemoveAt(i);
            removedCount++;
            i--;

            if (message.TryGet(ChatCompletionClient.Constants.ResponseFields.Role, out string role) == false)
                continue;

            if (role == ChatCompletionClient.Constants.RequestFields.RoleAssistantValue)
            {
                // Check if assistant message contains tool_calls
                if (message.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray toolCalls) &&
                    toolCalls != null)
                {
                    foreach (BlittableJsonReaderObject toolCall in toolCalls)
                    {
                        if (toolCall.TryGet(ChatCompletionClient.Constants.ResponseFields.Id, out string id) &&
                            string.IsNullOrWhiteSpace(id) == false)
                        {
                            toolCallIds.Add(id);
                        }
                    }
                }

                continue;
            }
            if (role == ChatCompletionClient.Constants.RequestFields.RoleToolValue)
            {
                if (message.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCallId, out string toolCallId) == false)
                    continue;

                toolCallIds.Remove(toolCallId);
            }
        }

        if (toolCallIds.Count > 0)
        {
            // Bound by messages.Count (current size after the first pass) — not n+1, which
            // is the original target and would over-shoot the now-shrunken list.
            for (int j = i; j < messages.Count; j++)
            {
                var message = messages[j];
                if (message.TryGet(ChatCompletionClient.Constants.ResponseFields.Role, out string role) == false ||
                    role != ChatCompletionClient.Constants.RequestFields.RoleToolValue ||
                    message.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCallId, out string toolCallId) == false)
                    continue;

                if (toolCallIds.Contains(toolCallId))
                {
                    toolCallIds.Remove(toolCallId);
                    messages.RemoveAt(j);
                    j--;

                    if (toolCallIds.Count == 0)
                        break;
                }
            }
        }
    }

    private async Task SummarizeAsync(JsonOperationContext context, ChatCompletionClient client, AiAgentConfiguration configuration, ConversationDocument oldChat, CancellationToken token)
    {
        var summarization = configuration.ChatTrimming.Tokens;
        var systemPrompt = oldChat.Messages.FirstOrDefault();
        if (systemPrompt == null)
            throw new InvalidOperationException("Cannot perform summarization: the conversation's original system prompt is null.");

        if (systemPrompt.TryGet(ChatCompletionClient.Constants.RequestFields.Content, out string _) == false)
            throw new InvalidOperationException($"Cannot perform summarization: the conversation's original system prompt has no '{ChatCompletionClient.Constants.RequestFields.Content}' field.");

        var beginningPrompt = string.IsNullOrEmpty(summarization.SummarizationTaskBeginningPrompt)
            ? database.Configuration.Ai.SummarizationTaskBeginningPrompt
            : summarization.SummarizationTaskBeginningPrompt;
        beginningPrompt += $" The original system prompt was: {systemPrompt}, the rest of follows";


        var messages = new List<BlittableJsonReaderObject>()
        {
            context.ReadObject(
                new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.RequestFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleSystemValue,
                    [ChatCompletionClient.Constants.RequestFields.Content] = beginningPrompt,
                }, "system/summary/msg"),
        };
        messages.AddRange(oldChat.Messages.Skip(1));

        var endPrompt = string.IsNullOrEmpty(summarization.SummarizationTaskEndPrompt)
            ? database.Configuration.Ai.SummarizationTaskEndPrompt
            : summarization.SummarizationTaskEndPrompt;

        messages.Add(context.ReadObject(
            new DynamicJsonValue
            {
                [ChatCompletionClient.Constants.RequestFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleUserValue,
                [ChatCompletionClient.Constants.RequestFields.Content] = endPrompt,
                [ChatCompletionClient.Constants.RequestFields.MaxCompletionToken] = summarization.MaxTokensAfterSummarization
            }, "system/summary/final/msg"));

        var usage = new AiUsage();
        var tools = client.GenerateTools(context, configuration, this);
        using var request = client.CreateCompletionRequest(context, messages, attachments: null, tools, useTools: false, streaming: false, schema: SummarizationOutputSchema);
        var result = await client.CompleteAsync(context, request, usage, trace: null, token);

        if (result.Result.TryGet(nameof(SummarizationSampleObject.Answer), out string messagesSummary) == false)
            throw new UnexpectedResponseException($"Unable to get a summary from response of agent '{oldChat.Agent}'.") { RequestId = null };

        oldChat.Messages.Clear();

        oldChat.Initialize(context, configuration, resetRemainingToolIterations: false, _maxModelIterationsPerCall);
        oldChat.AddMessage(context,
            context.ReadObject(
                new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.RequestFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleAssistantValue,
                    [ChatCompletionClient.Constants.RequestFields.Content] = summarization.ResultPrefix + messagesSummary,
                    [ConversationDocument.SummaryProperty] = true
                },
                "system/msg"), usage);

        oldChat.UpdateUsage(usage);
        OnUpdateUsage?.Invoke($"summary/{database.Name}", usage);

        oldChat.CurrentUsage = new AiUsage();
    }

    private bool TryGetUserTools(JsonOperationContext context, List<AiToolCall> toolCalls)
    {
        foreach (var call in toolCalls)
        {
            if (FindToolFrom(_configuration, call.Name) is not AiAgentToolAction)
                continue;

            _document.OpenActionCalls.Add(call.Id, new AiAgentActionRequest
            {
                ToolId = call.Id,
                Name = call.Name,
                Arguments = CreateParameters(context, call, _document.Parameters).ToString(),
                Type = AiAgentActionRequestType.UserAction
            });
        }

        foreach (var openActionCall in _document.OpenActionCalls)
        {
            if (openActionCall.Value.IsInternalToolCall() == false)
            {
                return true; //we have at least one user tool to handle
            }
        }
        // no user tools to handle - all were internal system actions/ there are no actions
        return false;
    }

    public static BlittableJsonReaderObject CreateParameters(JsonOperationContext context, AiToolCall call, BlittableJsonReaderObject parameters)
    {
        var args = context.Sync.ReadForMemory(call.Arguments, "call/args");
        if (parameters is null)
            return args;

        args.Modifications = new DynamicJsonValue();
        BlittableJsonReaderObject.PropertyDetails prop = default;
        for (int i = 0; i < parameters.Count; i++)
        {
            // Important: we *override* any parameter from the model with the user provided values
            // to ensure the safety & security of this feature. Model cannot override those values, period.
            parameters.GetPropertyByIndex(i, ref prop);
            args.Modifications[prop.Name] = GetAiConversationParameter(prop.Name, prop.Value).Value;
        }

        return context.ReadObject(args, "args");
    }

    public AiConnectionString GetAiConnectionString()
    {
        var name = _configuration.ConnectionStringName;
        using (server.ContextPool.AllocateOperationContext(out TransactionOperationContext serverCtx))
        using (serverCtx.OpenReadTransaction())
        {
            return server.Cluster.ReadRawDatabaseRecord(serverCtx, database.Name).GetAiConnectionString(name)
                   ?? throw new InvalidOperationException("Cannot find connection string: " + name);
        }
    }

    public Task<int> HandleQueryAndAgentCallsAsync(JsonOperationContext context, List<AiToolCall> toolCalls, CancellationToken token)
    {
        if (toolCalls == null || toolCalls.Count == 0)
            return Task.FromResult(0);

        Dictionary<string, List<(AiToolCall, DynamicJsonValue)>> reqs = [];
        foreach (var call in toolCalls)
        {
            switch (FindToolFrom(_configuration, call.Name))
            {
                case AiAgentToolQuery q:
                    BuildQueryRequest(context, _document, reqs, q, call);
                    break;
                case AiAgentToolSubAgent agent:
                    BuildAgentRequest(context, _document, call, agent, reqs);
                    break;
            }
        }

        if (reqs.Count is 0)
            return Task.FromResult(0);

        return ExecuteSubAgentAndQueryRequestsAsync(context, reqs, token);
    }

    private static void RemoveNonEssentialFieldsFromMetadata(BlittableJsonReaderArray queryResult)
    {
        foreach (BlittableJsonReaderObject doc in queryResult)
        {
            if (doc.TryGet(Client.Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
            {
                var modifications = metadata.Modifications = new DynamicJsonValue(metadata);
                modifications.Remove(Client.Constants.Documents.Metadata.ChangeVector);
                modifications.Remove(Client.Constants.Documents.Metadata.IndexScore);
                modifications.Remove(Client.Constants.Documents.Metadata.Counters);
                modifications.Remove(Client.Constants.Documents.Metadata.TimeSeries);
                modifications.Remove(Client.Constants.Documents.Metadata.Attachments);
                modifications.Remove(Client.Constants.Documents.Metadata.Flags);
                modifications.Remove(Client.Constants.Documents.Metadata.Projection);
                modifications.Remove(Client.Constants.Documents.Metadata.RavenClrType);
                modifications.Remove(Client.Constants.Documents.Metadata.Collection);
            }
        }
    }

    private void BuildQueryRequest(JsonOperationContext context, ConversationDocument document, Dictionary<string, List<(AiToolCall, DynamicJsonValue)>> reqs, AiAgentToolQuery q, AiToolCall call)
    {
        reqs.GetOrAdd(QueryVirtualSubConversationId).Add((call, new DynamicJsonValue
            {
                [nameof(GetRequest.Url)] = $"/databases/{database.Name}/queries",
                [nameof(GetRequest.Query)] = null,
                [nameof(GetRequest.Method)] = "POST",
                [nameof(GetRequest.Content)] = new DynamicJsonValue
                {
                    [nameof(IndexQuery.Query)] = q.Query,
                    [nameof(IndexQuery.QueryParameters)] = CreateParameters(context, call, document.Parameters)
                }
            }));
    }

    private object FindToolFrom(AiAgentConfiguration self, string name)
    {
        var query = self.FindQuery(name);
        if (query != null)
            return query;

        var subAgent = self.FindSubAgent(name);
        if (subAgent != null)
            return subAgent;

        var action = self.FindAction(name);
        return action;
    }

    public AiAgentConfiguration GetAiAgentConfiguration(string identifier)
    {
        using (server.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
        using (ctx.OpenReadTransaction())
        using (var record = server.Cluster.ReadRawDatabaseRecord(ctx, database.Name))
        {
            if (record.TryGetAiAgent(identifier, out var configuration) == false)
                throw new ArgumentException($"AI Agent '{identifier}' doesn't exists");

            return configuration;
        }
    }


    protected virtual async Task<string> TryPersistAsync(JsonOperationContext context, List<BlittableJsonReaderObject> historyDocs)
    {
        var changeVectorLsv = context.GetLazyString(_document.ChangeVector);
        var cmd = new PutConversationCommand(_document, historyDocs, changeVectorLsv, _configuration, database)
        {
            Attachments = _request.AttachmentCommands
        };
        await database.TxMerger.Enqueue(cmd);
        _document.ChangeVector = cmd.PutResult.ChangeVector;
        return cmd.PutResult.Id;
    }

    private async Task<string> GetDocumentIdAsync()
    {
        var id = _conversationId;
        if (id[^1] == '|')
        {
            var r = await server.GenerateClusterIdentityAsync(id, database.IdentityPartsSeparator, database.Name, _raftId ?? Guid.NewGuid().ToString());
            id = r.ClusterId;
        }

        return database.DocumentsStorage.DocumentPut.BuildDocumentId(id, database.DocumentsStorage.GenerateNextEtag(), out _);
    }

    private async Task<bool> TryHandleActionResponsesAsync(JsonOperationContext context, CancellationToken token)
    {
        if (_cancelPendingActionTools)
        {
            var cancelledActionResponses = new List<AiAgentActionResponse>();
            foreach (var actionCall in _document.OpenActionCalls)
            {
                var id = actionCall.Key;
                cancelledActionResponses.Add(new AiAgentActionResponse
                {
                    ToolId = id,
                    Content = "This action was canceled by the user"
                });
            }

            context.ReadObject(new DynamicJsonValue()
                {
                    ["array"] = new DynamicJsonArray(cancelledActionResponses.Select(x => x.ToJson()))
                }, "ai-agent/action-responses")
                .TryGet("array", out BlittableJsonReaderArray actionResponses);
            _request.ActionResponses = actionResponses;
        }

        var hasActionResponse = _request.ActionResponses is { Length: > 0 } ;
        var hasUserPrompt = RequestBody.HasUserPrompt(_request.Content) ||
                            _request.ArtificialActions is { Length: > 0 } || // equivalent to user prompt, since it is both tool & response in one shot
                            _request.Attachments is { Count: > 0 }; // Attachments-only request counts as enough "user input" (no text prompt required) to advance with the conversation.

        if (hasActionResponse && hasUserPrompt && _cancelPendingActionTools == false)
            throw new InvalidOperationException($"Cannot have a conversation '{_conversationId}' with open action calls and user prompt.");

        Dictionary<string, SubAgentActionResponse> subAgentsActions = null;

        if (_request.ActionResponses != null)
        {
            foreach (BlittableJsonReaderObject tool in _request.ActionResponses)
            {
                var t = JsonDeserializationClient.ActionResponse(tool);
                var split = t.ToolId.Split('$', 2); // split by first '$'
                var rootToolId = split[0];

                if (_document.OpenActionCalls.TryGetValue(rootToolId, out var action) == false)
                    throw new InvalidOperationException($"{rootToolId} is an unknown action ID for conversation '{_conversationId}'");

                if (action.IsInternalToolCall())
                {
                    continue;
                }

                if (action.SubConversationId == null)
                {
                    if (_document.OpenActionCalls.Remove(t.ToolId) == false)
                        throw new InvalidOperationException($"{t.ToolId} is an unknown action ID for conversation '{_conversationId}'");

                    _document.AddMessage(context, context.ReadObject(
                        new DynamicJsonValue
                        {
                            [ChatCompletionClient.Constants.ResponseFields.ToolCallId] = t.ToolId,
                            [ChatCompletionClient.Constants.ResponseFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleToolValue,
                            [ChatCompletionClient.Constants.ResponseFields.Content] = t.Content
                        },
                        "user/tool"), usage: null);
                }
                else
                {
                    subAgentsActions ??= new Dictionary<string, SubAgentActionResponse>();

                    // Aggregate sub-agent responses per root tool call (one group per sub-agent call).
                    var subAgent = GetOrAddSubAgentsActionResponses(subAgentsActions, action, rootToolId); // get or add from subAgentsActions

                    if (_cancelPendingActionTools)
                    {
                        continue;
                    }

                    var childToolId = split[1];
                    Debug.Assert(action.Type == AiAgentActionRequestType.SubAgent, "action.Type != AiAgentActionRequestType.SubAgent");

                    subAgent.Responses.Add(new AiAgentActionResponse
                    {
                        ToolId = childToolId, // sub call ID
                        Content = t.Content,
                    });
                }
            }

            await HandleSubAgentCallsAsync(context, subAgentsActions, token);
        }

        if (_request.ArtificialActions != null)
        {
            foreach (BlittableJsonReaderObject tool in _request.ArtificialActions)
            {
                var t = JsonDeserializationClient.AiAgentArtificialAction(tool);
                t.Validate();

                string id = Guid.NewGuid().ToString("N");
                _document.AddArtificialToolCall(context, [new AiToolCall(id, t.ToolId, "{}")]);
                _document.AddToolResponse(context, id, t.Content);
            }
        }

        if (_document.OpenActionCalls.Any(x => x.Value.Type != AiAgentActionRequestType.SubAgent) || _childUserCalls.Count > 0)
        {
            Debug.Assert(_document.OpenActionCalls.Any(x => x.Value.Type == AiAgentActionRequestType.SubAgent) == _childUserCalls.Count > 0,
                "_document.OpenActionCalls.Any(x => x.Value.Type == AiAgentActionRequestType.SubAgent) != _childUserCalls.Count > 0");
           
            foreach (var openCall in _document.OpenActionCalls.Values)
            {
                if (openCall.IsInternalToolCall() == false)
                {
                    // We have pending tool-call results from the user;
                    // skip reduction - persist the document now without history,
                    // ensuring we can recover if TalkAsync fails.
                    await TryPersistAsync(context, historyDocs: null);
                    return false;
                }
            }
            HandleInternalSystemActionsAsync(context, _document.OpenActionCalls.Values.ToList(), token);
        }

        if (hasActionResponse == false && hasUserPrompt == false && _request.Attachments == null)
            throw new InvalidOperationException($"Cannot have a conversation '{_conversationId}' without open action calls or user prompt.");

        if (RequestBody.HasUserPrompt(_request.Content))
        {
            _document.AddMessage(context, context.ReadObject(new DynamicJsonValue
            {
                [ChatCompletionClient.Constants.ResponseFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleUserValue,
                [ChatCompletionClient.Constants.ResponseFields.Content] = _request.Content
            }, "user/msg"), usage: null);
        }

        return true;
    }

    private string GetToolResultContent(BlittableJsonReaderArray result)
    {
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext clone))
        {
            RemoveNonEssentialFieldsFromMetadata(result);
            return result.Clone(clone).ToString();
        }
    }

    private async Task<int> ExecuteSubAgentAndQueryRequestsAsync(JsonOperationContext context, Dictionary<string, List<(AiToolCall Call, DynamicJsonValue Req)>> reqs, CancellationToken token)
    {
        List<Task<SubConversationResult>> tasks = [];
        foreach (var (conversationId, conversationReqs) in reqs)
        {
            _document.SubConversationIds.Add(conversationId);
            tasks.Add(ExecuteSingleSubConversationToolCallsAsync(conversationId, conversationReqs, token));
        }

        try
        {
            await Task.WhenAll(tasks);
        }
        catch
        {
            // explicitly ignoring this, since we'll handle the error after this
        }

        List<Exception> exceptions = new();
        var toolsIterations = 0;
        foreach (var t in tasks)
        {
            try
            {
                var r = await t;
                toolsIterations += r.ToolsIterations;
                using (r.Disposable)
                {
                    ProcessSubConversationResult(context, r);
                }
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        }

        if (exceptions.Count > 0)
            throw new AggregateException(exceptions).ExtractSingleInnerException();

        _document.RemainingToolIterations = int.Max(_document.RemainingToolIterations - toolsIterations, 0);
        return toolsIterations;
    }

    protected virtual void ProcessSubConversationResult(JsonOperationContext context, SubConversationResult r)
    {
        foreach (var m in r.Messages)
        {
            _document.AddMessage(context, m.Clone(context), usage: null);
        }

        foreach (var callId in r.OpenToolCallsToRemove)
        {
            _document.OpenActionCalls.Remove(callId, out _);
        }

        foreach (var (key, value) in r.ChildUserCalls)
        {
            AddChildrenUserCall(_childUserCalls, key, value);
            _document.Messages.Add(context.ReadObject(
                new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.ResponseFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleInternalValue,
                    [ChatCompletionClient.Constants.ResponseFields.Type] = "sub-agent-action-call",
                    [ChatCompletionClient.Constants.ResponseFields.Content] = $"[sub-agent called action-tool '{value.Name}']",
                    [ChatCompletionClient.Constants.ResponseFields.ToolName] = value.Name,
                    [ChatCompletionClient.Constants.ResponseFields.SubConversationId] = value.SubConversationId,
                }, "tool-call/sub-agent-action-tool"));
        }
    }

    private async Task<SubConversationResult> ExecuteSingleSubConversationToolCallsAsync(string conversationId, List<(AiToolCall Call, DynamicJsonValue Req)> requests, CancellationToken token)
    {
        IDisposable disposable = null;

        try
        {
            disposable = database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context);

            var result = new SubConversationResult(disposable);

            await foreach (var (getRequestResult, i) in ExecuteMultiRequestsAsync(context, new DynamicJsonArray(requests.Select(x => x.Req)), token))
            {
                var currentCall = requests[i].Call;
                var toolCall = FindToolFrom(_configuration, currentCall.Name);
                if (toolCall == null)
                    throw new InvalidOperationException($"Ai-Agent has no tool in name '{currentCall.Name}'");

                switch (toolCall)
                {
                    case AiAgentToolSubAgent:
                        try
                        {
                            var subAgentResult = getRequestResult.Invoke();
                            if (TryCloseSubAgentCall(context, conversationId, subAgentResult, currentCall, result) == false)
                                return result;
                        }
                        catch (MissingAiAgentParameterException)
                        {
                            // Missing parameter detected in sub-agent execution
                            throw;
                        }
                        catch (Exception e)
                        {
                            result.Messages.Add(context.ReadObject(
                                    new DynamicJsonValue
                                    {
                                        [ChatCompletionClient.Constants.ResponseFields.ToolCallId] = currentCall.Id,
                                        [ChatCompletionClient.Constants.ResponseFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleToolValue,
                                        [ChatCompletionClient.Constants.ResponseFields.Content] = "Error has been occurred during the tool call execution: " + AiConversation.AiActionContext<object>.CreateErrorMessageForLlm(e),
                                        [ChatCompletionClient.Constants.ResponseFields.SubConversationId] = conversationId,
                                    }, "tool-call/response"));
                            result.OpenToolCallsToRemove.Add(currentCall.Id);
                        }
                        break;
                    case AiAgentToolQuery:
                        var requestResult = getRequestResult.Invoke();
                        if (requestResult.TryGet(nameof(QueryResult.Results), out BlittableJsonReaderArray queryResult) is false)
                            throw new InvalidOperationException($"Query output is missing the '{nameof(QueryResult.Results)}' field. (Query - Id: {currentCall.Id}, Name: {currentCall.Name})");

                        result.Messages.Add(context.ReadObject(
                            new DynamicJsonValue
                            {
                                [ChatCompletionClient.Constants.ResponseFields.ToolCallId] = currentCall.Id,
                                [ChatCompletionClient.Constants.ResponseFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleToolValue,
                                [ChatCompletionClient.Constants.ResponseFields.Content] = GetToolResultContent(queryResult)
                            }, "tool-call/response"));
                        break;
                    default:
                        throw new InvalidOperationException(
                            $"Type mismatch for tool '{currentCall.Name}' in sub-conversation '{conversationId}'. " +
                            $"Expected type: '{nameof(AiAgentToolSubAgent)}' or '{nameof(AiAgentToolQuery)}', Actual type: '{toolCall.GetType().Name}'.");
                }
            }

            return result;
        }
        catch
        {
            disposable?.Dispose();
            throw;
        }
    }

    private async IAsyncEnumerable<(Func<BlittableJsonReaderObject>, int)> ExecuteMultiRequestsAsync(JsonOperationContext context, DynamicJsonArray reqs, [EnumeratorCancellation] CancellationToken token)
    {
        var multiGetHandler = new MultiGetHandler();
        multiGetHandler.Init(new RequestHandlerContext
        {
            Database = database,
            RavenServer = server.Server,
            HttpContext = new DefaultHttpContext()
            {
                RequestAborted = token
            }
        });

        multiGetHandler.HttpContext.Features.Set<IHttpAuthenticationFeature>(Authentication);

        using (var reqsBlittable = context.ReadObject(new DynamicJsonValue { ["Requests"] = reqs }, "ai-agent/multi-query"))
        using (var handler = new MultiGetHandlerProcessorForPost(multiGetHandler))
        using (var memoryStream = RecyclableMemoryStreamFactory.GetRecyclableStream())
        {
            token.ThrowIfCancellationRequested();
            await handler.ExecuteMultiGetAsync(context, reqsBlittable, memoryStream);
            memoryStream.Position = 0;
            using var resp = context.Sync.ReadForMemory(memoryStream, "query/response");
            if (resp.TryGet("Results", out BlittableJsonReaderArray results) is false)
                throw new InvalidOperationException("Missing Results from multi-get reply");

            for (int i = 0; i < results.Length; i++)
            {
                var response = (BlittableJsonReaderObject)results[i];
                if (response.TryGet(nameof(GetResponse.StatusCode), out int statusCode) == false)
                    throw new InvalidOperationException("Missing status code");
                if (response.TryGet(nameof(GetResponse.Result), out BlittableJsonReaderObject requestResult) is false)
                    throw new InvalidOperationException("Missing Result from query request output");

                yield return (() =>
                {
                    if (statusCode != 200)
                        throw ExceptionDispatcher.Get(requestResult, (HttpStatusCode)statusCode);
                    return requestResult;
                }, i);
            }
        }
    }

    public async Task<AiInternalConversationResult> HandleRequestAsync(
        DocumentsOperationContext context,
        CancellationToken token)
    {
        await InitializeDocumentAsync(context, token);

        if (await TryHandleActionResponsesAsync(context, token) is false)
            return AiInternalConversationResult.Default;

        return await TalkAsync(context, token: token);
    }

    public async Task<AiInternalConversationResult> HandleStreamingRequestAsync(
        DocumentsOperationContext context,
        Stream outputStream,
        string streamPropertyPath,
        CancellationToken token)
    {
        await InitializeDocumentAsync(context, token);

        if (await TryHandleActionResponsesAsync(context, token) is false)
            return AiInternalConversationResult.Default;
        
        await using var writer = new AsyncBlittableJsonTextWriter(context, outputStream);
        return await StreamingTalkAsync(context, streamPropertyPath, async (data) =>
        {
            using LazyStringValue s = context.GetLazyString(data.Span, longLived: false);
            writer.WriteString(s);
            writer.WriteRawString("\r\n"u8);
            await writer.FlushAsync(token);
        }, token: token);
    }

    private async Task ResolveDeferredAttachmentsAsync(List<AiAttachment> attachments, CancellationToken token)
    {
        if (attachments == null)
            return;

        var remote = database.DocumentsStorage.AttachmentsStorage.RemoteAttachmentsStorage;

        foreach (var attachment in attachments)
        {
            if (attachment.Source == AiAttachmentSource.Deferred)
            {
                var sw = Stopwatch.StartNew();
                // Resolve the attachment data asynchronously
                attachment.Data = await remote.GetAttachmentDataAsBase64Async(attachment.RemoteStorageId, attachment.Data, attachment.Type, token);
                attachment.DownloadDurationInMs = sw.ElapsedMilliseconds;
                attachment.Source = AiAttachmentSource.FromAttachment;
            }
        }
    }

    private static readonly string SummarizationOutputSchema = ChatCompletionClient.GetSchemaFromSampleObject(JsonConvert.SerializeObject(new SummarizationSampleObject()));

    private class SummarizationSampleObject
    {
        public string Answer = "Summary of the following chat messages history";
    }

    public virtual DynamicJsonValue GetConversationResponse(JsonOperationContext context, BlittableJsonReaderObject response, int toolsIterations)
    {
        return new DynamicJsonValue
        {
            [nameof(ConversationResult<object>.ConversationId)] = _conversationId,
            [nameof(ConversationResult<object>.ChangeVector)] = _document.ChangeVector,
            [nameof(ConversationResult<object>.Response)] = response,
            [nameof(ConversationResult<object>.ActionRequests)] = new DynamicJsonArray(GetUserActions()),
            [nameof(ConversationResult<object>.TotalUsage)] = _document.TotalUsage.ToJson(),
            [nameof(ConversationResult<object>.Usage)] = _document.CurrentUsage.ToJson(),
            [nameof(ConversationResult<object>.Elapsed)] = _elapsed,
            [nameof(ConversationResult<object>.ToolsIterations)] = toolsIterations
        };
    }

    private IEnumerable<DynamicJsonValue> GetUserActions()
    {
        foreach (var action in _childUserCalls)
        {
            action.Value.ToolId = $"{action.Value.ToolId}${action.Key}";
            yield return action.Value.ToJson();
        }

        foreach (var action in _document.OpenActionCalls)
        {
            if (action.Value.IsInternalToolCall())
                continue;

            if (action.Value.Type == AiAgentActionRequestType.SubAgent)
                continue;

            // replace with the actual tool call ID
            // for non-sub-agent user actions, it is identical
            action.Value.ToolId = action.Key;
            yield return action.Value.ToJson();
        }
    }
}
