using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Newtonsoft.Json;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI;
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

namespace Raven.Server.Documents.Handlers.AI.Agents;

public partial class ConversationHandler(ServerStore server, DocumentDatabase database)
{
    public const int DefaultMaxModelIterationsPerCall = 16;
    private const int DefaultMaxTokensBeforeSummarization = 32 * 1024;
    private const int DefaultMaxTokensAfterSummarization = 1024;
    private const string QueryVirtualSubConversationId = "_QueryTools_";

    protected ConversationDocument _document;
    private string _conversationId;
    private RequestBody _request;
    private AiAgentConfiguration _configuration;
    private string _changeVector;
    private string _raftId;
    protected int _maxModelIterationsPerCall;

    public required RavenServer.AuthenticateConnection Authentication;

    public void Initialize(AiAgentConfiguration configuration, string conversationId, RequestBody body, string changeVector, string raftId = null)
    {
        _conversationId = conversationId;
        _request = body;
        _configuration = configuration;
        _changeVector = changeVector;
        _raftId = raftId;
        _maxModelIterationsPerCall = GetMaxModelIterationsPerCall(body, configuration);
    }

    protected virtual async Task InitializeDocument(DocumentsOperationContext context)
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

            if (RequestBody.HasUserPrompt(_request.Content) == false)
            {
                throw new InvalidOperationException(
                    $"Cannot start a new conversation '{_conversationId}' without a user prompt.");
            }

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
                await HandleQueryAndAgentCallsAsync(context, queries);
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
        CancellationToken token = default)
    {
        using var talker = new Talker(this, context, _configuration, _document, firstStreamPropertyPath, streaming);
        return await RunInternalAsync(context, talker, token);
    }
        
    public async Task<AiInternalConversationResult> TalkAsync(
        JsonOperationContext context,
        CancellationToken token = default)
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

        AiResponse r = default;
        List<BlittableJsonReaderObject> historyDocs = default;
        bool shouldContinueConversation = true;
        var sw = Stopwatch.StartNew();

        var pendingAlertsDetails = new List<ExceededTokenThresholdDetails>();
        while (shouldContinueConversation)
        {
            using var request = talker.CreateCompletionRequest(_request.Attachments);

            database.ForTestingPurposes?.BeforeAiAgentTalk?.Invoke(talker.Document);

            r = await talker.RunAsync(database.DocumentsStorage.ContextPool, request, token);

            var currentTurnUsage = AiUsage.GetUsageDifference(talker.AiUsage, _document.CurrentUsage);

            _document.AddMessage(context, r.Message, currentTurnUsage);
            _document.UpdateUsage(talker.AiUsage);

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
                var iterations = await HandleQueryAndAgentCallsAsync(context, r.ToolCalls);
                toolsIterations += iterations;
                if (TryGetUserTools(context, r.ToolCalls))
                {
                    shouldContinueConversation = false;
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

    private async Task<BlittableJsonReaderObject> TryReduceChatSizeAsync(JsonOperationContext context, ChatCompletionClient client, AiUsage aiUsage, CancellationToken token)
    {
        var reduction = _configuration.ChatTrimming;
        if (reduction == null || _document.OpenActionCalls.Count > 0)
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
                if (truncateCount > 0)
                {
                    var chatBefore = reduction.History == null ? null : _document.ToHistoryBlittable(context, _configuration, historyExpiration);
                    _document.Messages.RemoveRange(1, truncateCount);
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
        var tools = ConversationDocument.GenerateTools(this, context, configuration);
        using var request = client.CreateCompletionRequest(context, messages, attachments: null, tools, useTools: false, streaming: false, schema: SummarizationOutputSchema);
        var result = await client.CompleteAsync(context, request, usage, token);

        if (result.Result.TryGet(nameof(SummarizationSampleObject.Answer), out string messagesSummary) == false)
            throw new UnexpectedResponseException($"Unable to get a summary from response of agent '{oldChat.Agent}'.") { RequestId = null };

        oldChat.Messages.Clear();

        oldChat.Initialize(context, configuration, resetRemainingToolIterations: false, _maxModelIterationsPerCall);
        oldChat.AddMessage(context,
            context.ReadObject(
                new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.RequestFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleAssistantValue,
                    [ChatCompletionClient.Constants.RequestFields.Content] = summarization.ResultPrefix + messagesSummary
                },
                "system/msg"), usage);

        oldChat.UpdateUsage(usage);
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

        return _document.OpenActionCalls.Count > 0;
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
            args.Modifications[prop.Name] = prop.Value;
        }

        return args;
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

    public Task<int> HandleQueryAndAgentCallsAsync(JsonOperationContext context, List<AiToolCall> toolCalls)
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

        return ExecuteMultiAgentAndQueryRequestsAsync(context, reqs);
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
        foreach (AiAgentToolQuery query in self.Queries ?? [])
        {
            if (query.Name == name)
                return query;
        }

        foreach (var agent in self.SubAgents ?? [])
        {
            if (agent.Identifier == name)
                return agent;
        }

        foreach (AiAgentToolAction action in self.Actions ?? [])
        {
            if (action.Name == name)
                return action;
        }

        return null;
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
        var cmd = new PutConversationCommand(_document, historyDocs, changeVectorLsv, _configuration, database);
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

    private async Task<bool> TryHandleActionResponses(JsonOperationContext context)
    {
        var hasActionResponse = _request.ActionResponses is { Length: > 0 } ;
        var hasUserPrompt = RequestBody.HasUserPrompt(_request.Content) || 
                            _request.ArtificialActions is { Length: > 0 }; // equivalent to user prompt, since it is both tool & response in one shot

        if (hasActionResponse && hasUserPrompt)
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

                if (action.SubConversation == null)
                {
                    _document.OpenActionCalls.Remove(rootToolId);
                    _document.AddMessage(context, context.ReadObject(
                        new DynamicJsonValue
                        {
                            ["tool_call_id"] = t.ToolId,
                            ["role"] = "tool",
                            ["content"] = t.Content
                        },
                        "user/tool"), usage: null);
                }
                else
                {
                    var childToolId = split[1];
                    Debug.Assert(action.Type == AiAgentActionRequestType.SubAgent, "action.Type != AiAgentActionRequestType.SubAgent");

                    subAgentsActions ??= new Dictionary<string, SubAgentActionResponse>();

                    var subAgent = GetOrAddSubAgentsActionResponses(subAgentsActions, action, rootToolId); // get or add from subAgentsActions
                    subAgent.Responses.Add(new AiAgentActionResponse
                    {
                        ToolId = childToolId, // sub call ID
                        Content = t.Content,
                    });
                }
            }

            await HandleSubAgentCalls(context, subAgentsActions);
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
            // We have pending tool-call results from the user;
            // skip reduction - persist the document now without history,
            // ensuring we can recover if TalkAsync fails.
            await TryPersistAsync(context, historyDocs: null);
            return false;
        }

        if (hasActionResponse == false && hasUserPrompt == false)
            throw new InvalidOperationException($"Cannot have a conversation '{_conversationId}' without open action calls or user prompt.");


        if (RequestBody.HasUserPrompt(_request.Content))
        {
            _document.AddMessage(context, context.ReadObject(new DynamicJsonValue
            {
                ["role"] = "user",
                ["content"] = _request.Content
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

    private async Task<int> ExecuteMultiAgentAndQueryRequestsAsync(JsonOperationContext context, Dictionary<string, List<(AiToolCall Call, DynamicJsonValue Req)>> reqs)
    {
        List<Task<SubConversationResult>> tasks = [];
        foreach (var (conversationId, conversationReqs) in reqs)
        {
            tasks.Add(ExecuteSingleSubConversationToolCallsAsync(conversationId, conversationReqs));
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
                    }
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

    private async Task<SubConversationResult> ExecuteSingleSubConversationToolCallsAsync(string conversationId, List<(AiToolCall Call, DynamicJsonValue Req)> requests)
    {
        IDisposable disposable = null;

        try
        {
            disposable = database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context);

            var result = new SubConversationResult(disposable);

            await foreach (var (getRequestResult, i) in ExecuteMultiRequestsAsync(context, new DynamicJsonArray(requests.Select(x => x.Req))))
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
                        catch (MissingAiAgentParameterException me)
                        {
                            // Missing parameter detected in sub-agent execution
                            // Wrapped to preserve context for nested sub-agent depth (>= 3 levels)
                            throw new MissingAiAgentParameterException($"Missing parameter on sub-conversation '{conversationId}'", me);
                        }
                        catch (Exception e)
                        {
                            result.Messages.Add(context.ReadObject(
                                    new DynamicJsonValue
                                    {
                                        ["tool_call_id"] = currentCall.Id,
                                        ["role"] = "tool",
                                        ["content"] = "Error has been occurred during the tool call execution: " + e.Message,
                                        ["subConversation"] = conversationId,
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
                                ["tool_call_id"] = currentCall.Id,
                                ["role"] = "tool",
                                ["content"] = GetToolResultContent(queryResult)
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

    private async IAsyncEnumerable<(Func<BlittableJsonReaderObject>, int)> ExecuteMultiRequestsAsync(JsonOperationContext context, DynamicJsonArray reqs)
    {
        var multiGetHandler = new MultiGetHandler();
        multiGetHandler.Init(new RequestHandlerContext
        {
            Database = database,
            RavenServer = server.Server,
            HttpContext = new DefaultHttpContext()
        });

        multiGetHandler.HttpContext.Features.Set<IHttpAuthenticationFeature>(Authentication);

        using (var reqsBlittable = context.ReadObject(new DynamicJsonValue { ["Requests"] = reqs }, "ai-agent/multi-query"))
        using (var handler = new MultiGetHandlerProcessorForPost(multiGetHandler))
        using (var memoryStream = RecyclableMemoryStreamFactory.GetRecyclableStream())
        {
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

    public async Task<AiInternalConversationResult> HandleRequest(
        DocumentsOperationContext context,
        CancellationToken token)
    {
        await InitializeDocument(context);

        if (await TryHandleActionResponses(context) is false)
            return AiInternalConversationResult.Default;

        return await TalkAsync(context, token: token);
    }

    public async Task<(string, AiUsage Usage)> HandleRequest(CancellationToken token)
    {
        using var _ = database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
        var r = await HandleRequest(context, token);
        return (r.Response.ToString(), r.Usage);
    }

    public async Task<AiInternalConversationResult> HandleStreamingRequest(
        DocumentsOperationContext context,
        Stream outputStream,
        string streamPropertyPath,
        CancellationToken token)
    {
        await InitializeDocument(context);

        if (await TryHandleActionResponses(context) is false)
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
            if (action.Value.Type == AiAgentActionRequestType.SubAgent)
                continue;

            // replace with the actual tool call ID
            // for non-sub-agent user actions, it is identical
            action.Value.ToolId = action.Key;
            yield return action.Value.ToJson();
        }
    }
}
