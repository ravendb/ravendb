using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.AI;

/// <summary>
/// Manages AI agents and conversation interactions in a specific RavenDB database.
/// </summary>
public class AiOperations
{
    private readonly string _databaseName;
    internal IDocumentStore _store;
    internal readonly MaintenanceOperationExecutor _executor;

    /// <summary>
    /// Initializes a new instance of <see cref="AiOperations"/> for a given document store and optional database name.
    /// </summary>
    /// <param name="store">The RavenDB document store.</param>
    /// <param name="databaseName">The name of the database. If null, uses the default database from the store.</param>
    public AiOperations(IDocumentStore store, string databaseName = null)
    {
        ValidationMethods.AssertNotNullOrEmpty(store, nameof(store));

        _databaseName = databaseName ?? store.Database;
        _store = store;
        _executor = _store.Maintenance.ForDatabase(_databaseName);
    }

    internal IDisposable AllocateOperationContext(out JsonOperationContext context) => _executor.RequestExecutor.ContextPool.AllocateOperationContext(out context);

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
    public Task<AiAgentConfigurationResult> CreateAgentAsync<TSchema>(AiAgentConfiguration configuration, TSchema sampleObject, CancellationToken token = default)
    {
        return _executor.SendAsync(AddOrUpdateAiAgentOperation.Create(configuration, sampleObject), token);
    }

    /// <summary>
    /// Creates or updates an AI agent configuration (with the given schema) on the database.
    /// </summary>
    /// <param name="configuration">The configuration to assign to the agent.</param>
    /// <returns>The result of the creation or update operation.</returns>
    public AiAgentConfigurationResult CreateAgent(AiAgentConfiguration configuration)
    {
        return AsyncHelpers.RunSync(() => CreateAgentAsync(configuration));
    }
    
    /// <summary>
    /// Asynchronously creates or updates an AI agent configuration (with the given schema) on the database.
    /// </summary>
    /// <param name="configuration">The configuration to assign to the agent.</param>
    /// <returns>The result of the creation or update operation.</returns>
    public Task<AiAgentConfigurationResult> CreateAgentAsync(AiAgentConfiguration configuration, CancellationToken token = default)
    {
        return _executor.SendAsync(new AddOrUpdateAiAgentOperation(configuration), token);
    }
    

    /// <summary>
    /// Creates or updates an AI agent configuration (with the given schema) on the database.
    /// </summary>
    /// <typeparam name="TSchema">The schema type the AI agent should use.</typeparam>
    /// <param name="configuration">The configuration to assign to the agent.</param>
    /// <returns>The result of the creation or update operation.</returns>
    public AiAgentConfigurationResult CreateAgent<TSchema>(AiAgentConfiguration configuration, TSchema sampleObject) where TSchema : new()
    {
        return AsyncHelpers.RunSync(() => CreateAgentAsync(configuration, sampleObject));
    }

    /// <summary>
    /// Asynchronously deletes an AI agent from the database.
    /// </summary>
    /// <param name="agentId">The ID of the AI agent to delete.</param>
    /// <returns>The result of the delete operation.</returns>
    public Task<AiAgentConfigurationResult> DeleteAgentAsync(string agentId, CancellationToken token = default)
    {
        return _executor.SendAsync(new DeleteAiAgentOperation(agentId), token);
    }

    /// <summary>
    /// Deletes an AI agent from the database.
    /// </summary>
    /// <param name="agentId">The ID of the AI agent to delete.</param>
    /// <returns>The result of the delete operation.</returns>
    public AiAgentConfigurationResult DeleteAgent(string agentId)
    {
        return AsyncHelpers.RunSync(() => DeleteAgentAsync(agentId));
    }

    /// <summary>
    /// Retrieves the AI agent configuration for a specific agent asynchronously.
    /// </summary>
    /// <param name="agentId">The ID of the AI agent to retrieve.</param>
    public async Task<AiAgentConfiguration> GetAgentAsync(string agentId, CancellationToken token = default)
    {
        var r = await _executor.SendAsync(new GetAiAgentsOperation(agentId), token).ConfigureAwait(false);
        return r.AiAgents?.SingleOrDefault();
    }

    /// <summary>
    /// Retrieves all AI agents and their configurations.
    /// </summary>
    /// <returns>A response containing all AI agents.</returns>
    public Task<GetAiAgentsResponse> GetAgentsAsync(CancellationToken token = default) => _executor.SendAsync(new GetAiAgentsOperation(), token);

    /// <summary>
    /// Retrieves the AI agent configuration for a specific agent.
    /// </summary>
    /// <param name="agentId">The ID of the AI agent to retrieve.</param>
    public AiAgentConfiguration GetAgent(string agentId) => AsyncHelpers.RunSync(() => GetAgentAsync(agentId));
    
    /// <summary>
    /// Retrieves all AI agents and their configurations.
    /// </summary>
    /// <returns>A response containing all AI agents.</returns>
    public GetAiAgentsResponse GetAgents() => AsyncHelpers.RunSync(() => GetAgentsAsync());

    /// <summary>
    /// Opens a conversation with an AI agent.
    /// </summary>
    /// <param name="agentId">The ID of the AI agent to start a conversation with.</param>
    /// <param name="conversationId">The unique identifier for the conversation.</param>
    /// <param name="creationOptions">Options for creating the conversation.</param>
    /// <param name="changeVector">An optional change vector for concurrency control.</param>
    public IAiConversationOperations Conversation(string agentId, string conversationId, AiConversationCreationOptions creationOptions, string changeVector = null) =>
        new AiConversation(this, agentId, conversationId, creationOptions, changeVector);

    internal IAiConversationOperations Conversation(string agentId, string conversationId, AiConversationCreationOptions creationOptions, bool? debug, string changeVector = null, bool cancelPendingActionTools = false) =>
        new AiConversation(this, agentId, conversationId, creationOptions, changeVector, debug, cancelPendingActionTools);

    /// <summary>
    /// Reads messages from an AI conversation. Returns the most recent messages by default.
    /// </summary>
    /// <param name="conversationId">The conversation document ID.</param>
    /// <param name="token">Cancellation token.</param>
    public Task<AiConversationMessagesResult> GetConversationMessagesAsync(string conversationId, CancellationToken token = default)
    {
        return _executor.SendAsync(new GetConversationMessagesOperation(conversationId), token);
    }

    /// <summary>
    /// Reads messages from an AI conversation with full control over paging and filtering.
    /// </summary>
    /// <param name="parameters">Parameters controlling paging (Before/After timestamps), page size, and view filter.</param>
    /// <param name="token">Cancellation token.</param>
    public Task<AiConversationMessagesResult> GetConversationMessagesAsync(GetConversationMessagesOptions parameters, CancellationToken token = default)
    {
        return _executor.SendAsync(new GetConversationMessagesOperation(parameters), token);
    }

    /// <summary>
    /// Reads messages from an AI conversation. Returns the most recent messages by default.
    /// </summary>
    /// <param name="conversationId">The conversation document ID.</param>
    public AiConversationMessagesResult GetConversationMessages(string conversationId)
    {
        return AsyncHelpers.RunSync(() => GetConversationMessagesAsync(conversationId));
    }

    /// <summary>
    /// Reads messages from an AI conversation with full control over paging and filtering.
    /// </summary>
    /// <param name="parameters">Parameters controlling paging (Before/After timestamps), page size, and view filter.</param>
    public AiConversationMessagesResult GetConversationMessages(GetConversationMessagesOptions parameters)
    {
        return AsyncHelpers.RunSync(() => GetConversationMessagesAsync(parameters));
    }
}
