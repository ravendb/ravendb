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
    private string _databaseName;
    internal IDocumentStore _store;
    internal readonly MaintenanceOperationExecutor _executor;

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

    internal IDisposable AllocateOperationContext(out JsonOperationContext context) => _store.GetRequestExecutor().ContextPool.AllocateOperationContext(out context);

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
    public async Task<AiAgentConfigurationResult> CreateAgentAsync<TSchema>(AiAgentConfiguration configuration, TSchema schema, CancellationToken token = default) where TSchema : new()
    {
        return await _executor.SendAsync(AddOrUpdateAiAgentOperation.Create(configuration, schema), token).ConfigureAwait(false);
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
    public async Task<AiAgentConfigurationResult> CreateAgentAsync(AiAgentConfiguration configuration, CancellationToken token = default)
    {
        return await _executor.SendAsync(new AddOrUpdateAiAgentOperation(configuration), token).ConfigureAwait(false);
    }
    

    /// <summary>
    /// Creates or updates an AI agent configuration (with the given schema) on the database.
    /// </summary>
    /// <typeparam name="TSchema">The schema type the AI agent should use.</typeparam>
    /// <param name="configuration">The configuration to assign to the agent.</param>
    /// <returns>The result of the creation or update operation.</returns>
    public AiAgentConfigurationResult CreateAgent<TSchema>(AiAgentConfiguration configuration, TSchema schema) where TSchema : new()
    {
        return AsyncHelpers.RunSync(() => CreateAgentAsync(configuration, schema));
    }

    /// <summary>
    /// Retrieves the AI agent configuration for a specific agent asynchronously.
    /// </summary>
    /// <param name="agentId">The ID of the AI agent to retrieve.</param>
    public async Task<AiAgentConfiguration> GetAgentAsync(string agentId, CancellationToken token = default)
    {
        var r = await _executor.SendAsync(new GetAiAgentOperation(agentId), token).ConfigureAwait(false);
        return r.AiAgents?.FirstOrDefault();
    }

    /// <summary>
    /// Retrieves all AI agents and their configurations.
    /// </summary>
    /// <returns>A response containing all AI agents.</returns>
    public Task<GetAiAgentsResponse> GetAgentsAsync(CancellationToken token = default) => _executor.SendAsync(new GetAiAgentOperation(), token);

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
}
