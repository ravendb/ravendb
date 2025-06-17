using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Client.Documents.AI;

/// <summary>
/// Manages AI agents and chat interactions in a specific RavenDB database.
/// </summary>
public class DatabaseAiAgents
{
    private string _databaseName;
    private IDocumentStore _store;

    /// <summary>
    /// Initializes a new instance of <see cref="DatabaseAiAgents"/> for a given document store and optional database name.
    /// </summary>
    /// <param name="store">The RavenDB document store.</param>
    /// <param name="databaseName">The name of the database. If null, uses the default database from the store.</param>
    public DatabaseAiAgents(IDocumentStore store, string databaseName = null)
    {
        _databaseName = databaseName ?? store.Database;
        _store = store;
    }

    /// <summary>
    /// Returns a <see cref="DatabaseAiAgents"/> for a different database.
    /// </summary>
    /// <param name="databaseName">The name of the target database.</param>
    /// <returns>A new or existing <see cref="DatabaseAiAgents"/> instance.</returns>
    public DatabaseAiAgents ForDatabase(string databaseName)
    {
        if (string.Equals(_databaseName, databaseName, StringComparison.OrdinalIgnoreCase))
            return this;

        return new DatabaseAiAgents(_store, databaseName);
    }

    /// <summary>
    /// Asynchronously creates or updates an AI agent configuration (with the given schema) on the database.
    /// </summary>
    /// <typeparam name="TSchema">The schema type the AI agent should use.</typeparam>
    /// <param name="agentName">The unique name of the AI agent for create an agent, or name of an existed agent for update an agent.</param>
    /// <param name="configuration">The configuration to assign to the agent.</param>
    /// <returns>The result of the creation or update operation.</returns>
    public async Task<AiAgentConfigurationResult> CreateAgentAsync<TSchema>(string agentName, AiAgentConfiguration configuration) where TSchema : new()
    {
        return await _store.Maintenance.ForDatabase(_databaseName).SendAsync(new AddOrUpdateAiAgentOperation<TSchema>(agentName, configuration)).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates or updates an AI agent configuration (with the given schema) on the database.
    /// </summary>
    /// <typeparam name="TSchema">The schema type the AI agent should use.</typeparam>
    /// <param name="agentName">The unique name of the AI agent for create an agent, or name of an existed agent for update an agent.</param>
    /// <param name="configuration">The configuration to assign to the agent.</param>
    /// <returns>The result of the creation or update operation.</returns>
    public AiAgentConfigurationResult CreateAgent<TSchema>(string agentName, AiAgentConfiguration configuration) where TSchema : new()
    {
        return _store.Maintenance.ForDatabase(_databaseName).Send(new AddOrUpdateAiAgentOperation<TSchema>(agentName, configuration));
    }

    /// <summary>
    /// Asynchronously starts a new chat with an AI agent using a fluent parameter builder.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat.</typeparam>
    /// <param name="agentName">The name of the AI agent to chat with.</param>
    /// <param name="prompt">The initial user prompt.</param>
    /// <param name="func">A builder function to define RAG parameters.</param>
    /// <returns>The result of the chat.</returns>
    public Task<ChatResult<TSchema>> StartChatAsync<TSchema>(string agentName, string prompt, Func<AiAgentParametersBuilder, AiAgentParametersBuilder> func) where TSchema : new()
    {
        var builder = func.Invoke(new AiAgentParametersBuilder());
        var parameters = builder.GetParameters();
        return StartChatAsync<TSchema>(agentName, prompt, parameters);
    }

    /// <summary>
    /// Starts a new chat with an AI agent using a fluent parameter builder.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat.</typeparam>
    /// <param name="agentName">The name of the AI agent to chat with.</param>
    /// <param name="prompt">The initial user prompt.</param>
    /// <param name="func">A builder function to define RAG parameters.</param>
    /// <returns>The result of the chat.</returns>
    public ChatResult<TSchema> StartChat<TSchema>(string agentName, string prompt, Func<AiAgentParametersBuilder, AiAgentParametersBuilder> func) where TSchema : new()
    {
        var builder = func.Invoke(new AiAgentParametersBuilder());
        var parameters = builder.GetParameters();
        return StartChat<TSchema>(agentName, prompt, parameters);
    }

    /// <summary>
    /// Asynchronously starts a new chat with an AI agent using a dictionary of parameters.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat.</typeparam>
    /// <param name="agentName">The name of the AI agent to chat with.</param>
    /// <param name="prompt">The initial user prompt.</param>
    /// <param name="parameters">Optional dictionary of chat parameters.</param>
    /// <returns>The result of the chat.</returns>
    public async Task<ChatResult<TSchema>> StartChatAsync<TSchema>(string agentName, string prompt, Dictionary<string, object> parameters = null) where TSchema : new()
    {
        return await _store.Maintenance.ForDatabase(_databaseName).SendAsync(new StartChatOperation<TSchema>(agentName, prompt, parameters)).ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronously starts a new chat with an AI agent using a dictionary of parameters.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat.</typeparam>
    /// <param name="agentName">The name of the AI agent to chat with.</param>
    /// <param name="prompt">The initial user prompt.</param>
    /// <param name="parameters">Optional dictionary of chat parameters.</param>
    /// <returns>The result of the chat.</returns>
    public ChatResult<TSchema> StartChat<TSchema>(string agentName, string prompt, Dictionary<string, object> parameters = null) where TSchema : new()
    {
        return _store.Maintenance.ForDatabase(_databaseName).Send(new StartChatOperation<TSchema>(agentName, prompt, parameters));
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
        return _store.Maintenance.ForDatabase(_databaseName).Send(new ResumeChatOperation<TSchema>(chatId, userPrompt: prompt));
    }

    /// <summary>
    /// Asynchronously continues a chat using a fluent parameter builder.
    /// Also update the chat with the new prompt and response.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat.</typeparam>
    /// <param name="chatId">The ID of the existing chat.</param>
    /// <param name="prompt">The next user prompt in the conversation.</param>
    /// <returns>The result of the continued chat.</returns>
    public async Task<ChatResult<TSchema>> ContinueChatAsync<TSchema>(string chatId, string prompt) where TSchema : new()
    {
        return await _store.Maintenance.ForDatabase(_databaseName).SendAsync(new ResumeChatOperation<TSchema>(chatId, userPrompt: prompt)).ConfigureAwait(false);
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
        return _store.Maintenance.ForDatabase(_databaseName).Send(new ResumeChatOperation<TSchema>(chatId, toolResponses: toolResponses));
    }

    /// <summary>
    /// Asynchronously continues a chat using a fluent parameter builder.
    /// Also update the chat with the new prompt and response.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat.</typeparam>
    /// <param name="chatId">The ID of the existing chat.</param>
    /// <param name="toolResponses"> A list of tool responses corresponding to the previous 'ToolActions' requests made by the agent. </param>
    /// <returns>The result of the continued chat.</returns>
    public async Task<ChatResult<TSchema>> ContinueChatAsync<TSchema>(string chatId, List<ToolResponse> toolResponses) where TSchema : new()
    {
        return await _store.Maintenance.ForDatabase(_databaseName).SendAsync(new ResumeChatOperation<TSchema>(chatId, toolResponses: toolResponses)).ConfigureAwait(false);
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
