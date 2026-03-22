using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Newtonsoft.Json;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.Handlers.Processors.MultiGet;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class ConversationHandler(ServerStore server, DocumentDatabase database)
{
    internal static Action<string, AiUsage> OnUpdateUsage;

    public const int DefaultMaxModelIterationsPerCall = 16;
    private const int DefaultMaxTokensBeforeSummarization = 32 * 1024;
    private const int DefaultMaxTokensAfterSummarization = 1024;

    protected ConversationDocument _document;
    private string _conversationId;
    protected RequestBody _request;
    private AiAgentConfiguration _configuration;
    private string _changeVector;
    private string _raftId;
    private int _maxModelIterationsPerCall;

    public required RavenServer.AuthenticateConnection Authentication;
    public void Initialize(AiAgentConfiguration configuration, string conversationId, RequestBody body, string changeVector, string raftId = null)
    {
        _conversationId = conversationId;
        _request = body;
        _configuration = configuration;
        _changeVector = changeVector;
        _raftId = raftId;
        _maxModelIterationsPerCall = configuration.MaxModelIterationsPerCall ?? DefaultMaxModelIterationsPerCall;
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

            if (_request.CreationOptions.ExpirationInSec.HasValue)
            {
                _document.Expires = TimeSpan.FromSeconds(_request.CreationOptions.ExpirationInSec.Value);
            }

            _document.Initialize(context, _configuration);
            if (_document.InitialQueries(context, _configuration) is { } queries)
            {
                // run initial tool calls...
                await HandleQueryToolCallsAsync(context, queries);
            }
        }
        else
        {
            _document = ConversationDocument.ToDocument(_conversationId, conversation.Data, _configuration);

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

    public async Task<(BlittableJsonReaderObject Response, AiUsage Usage)> StreamingTalkAsync(
        JsonOperationContext context,
        string firstStreamPropertyPath,
        Func<Memory<byte>, Task> streaming,
        CancellationToken token = default)
    {
        using var talker = new Talker(this, context, _configuration, _document, firstStreamPropertyPath, streaming);
        return await RunInternalAsync(context, talker, token);
    }
        
    public async Task<(BlittableJsonReaderObject Response, AiUsage Usage)> TalkAsync(
        JsonOperationContext context,
        CancellationToken token = default)
    {
        using var talker = new Talker(this, context, _configuration, _document, firstStreamPropertyPath: null, streaming: null);
        return await RunInternalAsync(context, talker, token);
    }

    private TimeSpan _elapsed;

    private async Task<(BlittableJsonReaderObject Response, AiUsage Usage)> RunInternalAsync(
        JsonOperationContext context,
        Talker talker, CancellationToken token)
    {
        talker.Init();

        AiResponse r = default;
        List<BlittableJsonReaderObject> historyDocs = default;
        bool shouldContinueConversation = true;
        var sw = Stopwatch.StartNew();

        var pendingAlertsDetails = new List<ExceededTokenThresholdDetails>();
        while (shouldContinueConversation)
        {
            using var request = talker.CreateCompletionRequest(_request.Attachments);

            r = await talker.RunAsync(database.DocumentsStorage.ContextPool, request, token);
            
            var currentTurnUsage = AiUsage.GetUsageDifference(talker.AiUsage, _document.CurrentUsage);

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

            if (r.Type is AiResponseType.Result)
            {
                _document.RemainingToolIterations = _maxModelIterationsPerCall;
                shouldContinueConversation = false;
            }
            else
            {
                await HandleQueryToolCallsAsync(context, r.ToolCalls);
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
        return (r.Result, talker.AiUsage);
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
        var tools = ConversationDocument.GenerateTools(context, configuration);
        using var request = client.CreateCompletionRequest(context, messages, attachments: null, tools, useTools: false, streaming: false, schema: SummarizationOutputSchema);
        var result = await client.CompleteAsync(context, request, usage, token);

        if (result.Result.TryGet(nameof(SummarizationSampleObject.Answer), out string messagesSummary) == false)
            throw new UnexpectedResponseException($"Unable to get a summary from response of agent '{oldChat.Agent}'.") { RequestId = null };

        oldChat.Messages.Clear();

        oldChat.Initialize(context, configuration, resetRemainingToolIterations: false);

        oldChat.AddMessage(context,
            context.ReadObject(
                new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.RequestFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleAssistantValue,
                    [ChatCompletionClient.Constants.RequestFields.Content] = summarization.ResultPrefix + messagesSummary
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
            if (_configuration.FindAction(call.Name) == null)
                continue;

            _document.OpenActionCalls.Add(call.Id,
                new AiAgentActionRequest { ToolId = call.Id, Name = call.Name, Arguments = CreateParameters(context, call, _document.Parameters).ToString() });
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

    public async Task HandleQueryToolCallsAsync(JsonOperationContext context, List<AiToolCall> toolCalls)
    {
        if (toolCalls == null || toolCalls.Count == 0)
            return;

        DynamicJsonArray reqs = [];
        List<string> toolCallsIds = [];
        var queryUrl = $"/databases/{database.Name}/queries";
        foreach (var call in toolCalls)
        {
            var q = _configuration.FindQuery(call.Name);
            if (q is null)
                continue;

            toolCallsIds.Add(call.Id);
            reqs.Add(new DynamicJsonValue
            {
                [nameof(GetRequest.Url)] = queryUrl,
                [nameof(GetRequest.Query)] = null,
                [nameof(GetRequest.Method)] = "POST",
                [nameof(GetRequest.Content)] = new DynamicJsonValue
                {
                    [nameof(IndexQuery.Query)] = q.Query,
                    [nameof(IndexQuery.QueryParameters)] = CreateParameters(context, call, _document.Parameters)
                }
            });
        }

        if (reqs.Count == 0)
            return;

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

            if (TrafficWatchManager.HasRegisteredClients)
                RavenServerStartup.LogTrafficWatch(multiGetHandler.HttpContext, elapsedMilliseconds: 0, database.Name);

            using var resp = context.Sync.ReadForMemory(memoryStream, "query/response");
            if (resp.TryGet("Results", out BlittableJsonReaderArray results) is false)
                throw new InvalidOperationException("Missing Results from multi-get reply");

            for (int i = 0; i < results.Length; i++)
            {
                var queryResponse = (BlittableJsonReaderObject)results[i];
                if (queryResponse.TryGet(nameof(GetResponse.StatusCode), out int statusCode) == false)
                    throw new InvalidOperationException("Missing status code");
                if (queryResponse.TryGet(nameof(GetResponse.Result), out BlittableJsonReaderObject queryResponseResult) is false)
                    throw new InvalidOperationException("Missing Result from query request output");

                if (statusCode != 200)
                    throw ExceptionDispatcher.Get(queryResponseResult, (HttpStatusCode)statusCode);

                if (queryResponseResult.TryGet(nameof(QueryResult.Results), out BlittableJsonReaderArray queryResult) is false)
                    throw new InvalidOperationException("Missing Results from query output");

                RemoveNonEssentialFieldsFromMetadata(queryResult);

                _document.AddMessage(context, context.ReadObject(
                    new DynamicJsonValue
                    {
                        ["tool_call_id"] = toolCallsIds[i],
                        ["role"] = "tool",
                        ["content"] = queryResult.Clone().ToString()
                    }, "tool-call/response"), usage: null);
            }
        }
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

    protected virtual async Task<string> TryPersistAsync(JsonOperationContext context, List<BlittableJsonReaderObject> historyDocs)
    {
        if (_conversationId[^1] == '|')
        {
            var r = await server.GenerateClusterIdentityAsync(_conversationId, database.IdentityPartsSeparator, database.Name, _raftId ?? Guid.NewGuid().ToString());
            _conversationId = r.ClusterId;
        }

        var changeVectorLsv = context.GetLazyString(_document.ChangeVector);
        var cmd = new PutConversationCommand(_conversationId, _document, historyDocs, changeVectorLsv, _configuration, database);
        await database.TxMerger.Enqueue(cmd);
        _document.ChangeVector = cmd.PutResult.ChangeVector;
        return cmd.PutResult.Id;
    }


    private async Task<bool> TryHandleActionResponses(JsonOperationContext context)
    {
        var hasActionResponse = _request.ActionResponses is { Length: > 0 } ;
        var hasUserPrompt = RequestBody.HasUserPrompt(_request.Content) || 
                            _request.ArtificialActions is { Length: > 0 }; // equivalent to user prompt, since it is both tool & response in one shot

        if (hasActionResponse && hasUserPrompt)
            throw new InvalidOperationException($"Cannot have a conversation '{_conversationId}' with open action calls and user prompt.");

        if (_request.ActionResponses != null)
        {
            foreach (BlittableJsonReaderObject tool in _request.ActionResponses)
            {
                var t = JsonDeserializationClient.ActionResponse(tool);
                if (_document.OpenActionCalls.Remove(t.ToolId) == false)
                    throw new InvalidOperationException($"{t.ToolId} is an unknown action ID for conversation '{_conversationId}'");

                _document.AddToolResponse(context, t.ToolId, t.Content);
            }
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

        if (_document.OpenActionCalls.Count > 0)
        {
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

    public async Task<(BlittableJsonReaderObject Response, AiUsage Usage)> HandleRequest(
        DocumentsOperationContext context,
        CancellationToken token)
    {
        await InitializeDocument(context);

        if (await TryHandleActionResponses(context) is false)
            return default;

        return await TalkAsync(context, token: token);
    }

    public async Task<(BlittableJsonReaderObject Response, AiUsage Usage)> HandleStreamingRequest(
        DocumentsOperationContext context,
        Stream outputStream,
        string streamPropertyPath,
        CancellationToken token)
    {
        await InitializeDocument(context);

        if (await TryHandleActionResponses(context) is false)
            return default;

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

    public virtual DynamicJsonValue GetConversationResponse(JsonOperationContext context, BlittableJsonReaderObject response)
    {
        return new DynamicJsonValue
        {
            [nameof(ConversationResult<object>.ConversationId)] = _conversationId,
            [nameof(ConversationResult<object>.ChangeVector)] = _document.ChangeVector,
            [nameof(ConversationResult<object>.Response)] = response,
            [nameof(ConversationResult<object>.ActionRequests)] = new DynamicJsonArray(_document.OpenActionCalls.Select(t => t.Value.ToJson())),
            [nameof(ConversationResult<object>.TotalUsage)] = _document.TotalUsage.ToJson(),
            [nameof(ConversationResult<object>.Usage)] = _document.CurrentUsage.ToJson(),
            [nameof(ConversationResult<object>.Elapsed)] = _elapsed
        };
    }
}
