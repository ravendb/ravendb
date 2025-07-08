using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Util;

namespace Raven.Client.Documents.AI;

/// <summary>
/// Manages AI agents and chat interactions in a specific RavenDB database.
/// </summary>
public class AiOperations
{
    private string _databaseName;
    private IDocumentStore _store;
    private readonly MaintenanceOperationExecutor _executor;

    /// <summary>
    /// Initializes a new instance of <see cref="AiOperations"/> for a given document store and optional database name.
    /// </summary>
    /// <param name="store">The RavenDB document store.</param>
    /// <param name="databaseName">The name of the database. If null, uses the default database from the store.</param>
    public AiOperations(IDocumentStore store, string databaseName = null)
    {
        _databaseName = databaseName ?? store.Database;
        _store = store;
        _executor = _store.Maintenance.ForDatabase(_databaseName);
    }

    /// <summary>
    /// Returns a <see cref="AiOperations"/> for a different database.
    /// </summary>
    /// <param name="databaseName">The name of the target database.</param>
    /// <returns>A new or existing <see cref="AiOperations"/> instance.</returns>
    public AiOperations ForDatabase(string databaseName)
    {
        if (string.Equals(_databaseName, databaseName, StringComparison.OrdinalIgnoreCase))
            return this;

        return new AiOperations(_store, databaseName);
    }

    /// <summary>
    /// Asynchronously creates or updates an AI agent configuration (with the given schema) on the database.
    /// </summary>
    /// <typeparam name="TSchema">The schema type the AI agent should use.</typeparam>
    /// <param name="configuration">The configuration to assign to the agent.</param>
    /// <returns>The result of the creation or update operation.</returns>
    public async Task<AiAgentConfigurationResult> CreateAgentAsync<TSchema>(AiAgentConfiguration configuration, CancellationToken token = default) where TSchema : new()
    {
        return await _executor.SendAsync(new AddOrUpdateAiAgentOperation<TSchema>(configuration), token).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates or updates an AI agent configuration (with the given schema) on the database.
    /// </summary>
    /// <typeparam name="TSchema">The schema type the AI agent should use.</typeparam>
    /// <param name="configuration">The configuration to assign to the agent.</param>
    /// <returns>The result of the creation or update operation.</returns>
    public AiAgentConfigurationResult CreateAgent<TSchema>(AiAgentConfiguration configuration) where TSchema : new()
    {
        return AsyncHelpers.RunSync(() => CreateAgentAsync<TSchema>(configuration));
    }

    /// <summary>
    /// Asynchronously starts a new chat with an AI agent using a fluent parameter builder.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat.</typeparam>
    /// <param name="identifier">The identifier of the AI agent to chat with.</param>
    /// <param name="prompt">The initial user prompt.</param>
    /// <param name="builder">A builder function to define RAG parameters.</param>
    /// <returns>The result of the chat.</returns>
    public Task<ChatResult<TSchema>> StartChatAsync<TSchema>(string identifier, string prompt, Func<AiAgentParametersBuilder, AiAgentParametersBuilder> builder, CancellationToken token = default) where TSchema : new()
    {
        var parameters = builder.Invoke(new AiAgentParametersBuilder()).GetParameters();
        return StartChatAsync<TSchema>(identifier, prompt, parameters, token);
    }

    /// <summary>
    /// Starts a new chat with an AI agent using a fluent parameter builder.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat.</typeparam>
    /// <param name="identifier">The identifier of the AI agent to chat with.</param>
    /// <param name="prompt">The initial user prompt.</param>
    /// <param name="builder">A builder function to define RAG parameters.</param>
    /// <returns>The result of the chat.</returns>
    public ChatResult<TSchema> StartChat<TSchema>(string identifier, string prompt, Func<AiAgentParametersBuilder, AiAgentParametersBuilder> builder) where TSchema : new()
    {
        return AsyncHelpers.RunSync(() => StartChatAsync<TSchema>(identifier, prompt, builder));
    }

    /// <summary>
    /// Asynchronously starts a new chat with an AI agent using a dictionary of parameters.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat.</typeparam>
    /// <param name="identifier">The identifier of the AI agent to chat with.</param>
    /// <param name="prompt">The initial user prompt.</param>
    /// <param name="parameters">Optional dictionary of chat parameters.</param>
    /// <returns>The result of the chat.</returns>
    public async Task<ChatResult<TSchema>> StartChatAsync<TSchema>(string identifier, string prompt, Dictionary<string, object> parameters = null, CancellationToken token = default) where TSchema : new()
    {
        return await _executor.SendAsync(new StartChatOperation<TSchema>(identifier, prompt, parameters), token).ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronously starts a new chat with an AI agent using a dictionary of parameters.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat.</typeparam>
    /// <param name="identifier">The identifier of the AI agent to chat with.</param>
    /// <param name="prompt">The initial user prompt.</param>
    /// <param name="parameters">Optional dictionary of chat parameters.</param>
    /// <returns>The result of the chat.</returns>
    public ChatResult<TSchema> StartChat<TSchema>(string identifier, string prompt, Dictionary<string, object> parameters = null) where TSchema : new()
    {
        return AsyncHelpers.RunSync(() => StartChatAsync<TSchema>(identifier, prompt, parameters));
    }

    /// <summary>
    /// Continues a chat using a fluent parameter builder.
    /// Also update the chat with the new prompt and response.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat.</typeparam>
    /// <param name="chatId">The ID of the existing chat.</param>
    /// <param name="prompt">The next user prompt in the conversation.</param>
    /// <returns>The result of the continued chat.</returns>
    public ChatResult<TSchema> ContinueChat<TSchema>(string chatId, string prompt) where TSchema : new()
    {
        return AsyncHelpers.RunSync(() => ContinueChatAsync<TSchema>(chatId, prompt));
    }

    /// <summary>
    /// Asynchronously continues a chat using a fluent parameter builder.
    /// Also update the chat with the new prompt and response.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat.</typeparam>
    /// <param name="chatId">The ID of the existing chat.</param>
    /// <param name="prompt">The next user prompt in the conversation.</param>
    /// <returns>The result of the continued chat.</returns>
    public async Task<ChatResult<TSchema>> ContinueChatAsync<TSchema>(string chatId, string prompt, CancellationToken token = default) where TSchema : new()
    {
        return await _executor.SendAsync(new ResumeChatOperation<TSchema>(chatId, userPrompt: prompt), token).ConfigureAwait(false);
    }

    /// <summary>
    /// Continues a chat using a fluent parameter builder.
    /// Also update the chat with the new prompt and response.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat.</typeparam>
    /// <param name="chatId">The ID of the existing chat.</param>
    /// <param name="toolResponses"> A list of tool responses corresponding to the previous 'ToolActions' requests made by the agent. </param>
    /// <returns>The result of the continued chat.</returns>
    public ChatResult<TSchema> ContinueChat<TSchema>(string chatId, List<ToolResponse> toolResponses) where TSchema : new()
    {
        return AsyncHelpers.RunSync(() => ContinueChatAsync<TSchema>(chatId, toolResponses));
    }

    /// <summary>
    /// Asynchronously continues a chat using a fluent parameter builder.
    /// Also update the chat with the new prompt and response.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat.</typeparam>
    /// <param name="chatId">The ID of the existing chat.</param>
    /// <param name="toolResponses"> A list of tool responses corresponding to the previous 'ToolActions' requests made by the agent. </param>
    /// <returns>The result of the continued chat.</returns>
    public async Task<ChatResult<TSchema>> ContinueChatAsync<TSchema>(string chatId, List<ToolResponse> toolResponses, CancellationToken token = default) where TSchema : new()
    {
        return await _executor.SendAsync(new ResumeChatOperation<TSchema>(chatId, toolResponses: toolResponses), token).ConfigureAwait(false);
    }

    public class AiAgentParametersBuilder
    {
        private readonly Dictionary<string, object> _parameters = new();

        public AiAgentParametersBuilder AddParameter(string key, string value)
        {
            _parameters[key] = value;
            return this;
        }

        public Dictionary<string, object> GetParameters() => _parameters.Count == 0 ? null : _parameters;
    }
}
