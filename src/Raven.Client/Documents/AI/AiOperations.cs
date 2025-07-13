using System;
using System.Collections.Generic;
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
    /// Create starts a new conversation with an AI agent using a dictionary of parameters.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the conversation response.</typeparam>
    /// <param name="agentId">The ID of the AI agent to conversation with.</param>
    /// <param name="parameters">Required conversation parameters.</param>
    public IConversationOperations<TSchema> StartConversation<TSchema>(string agentId, Dictionary<string, object> parameters) where TSchema : new() => new Conversation<TSchema>(this, agentId, parameters);
    
    /// <summary>
    /// Create a new conversation with an AI agent using a dictionary of parameters.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the conversation response.</typeparam>
    /// <param name="agentId">The ID of the AI agent to conversation with.</param>
    /// <param name="builder">A builder to define the required conversation parameters.</param>
    public IConversationOperations<TSchema> StartConversation<TSchema>(string agentId, Action<IAiAgentParametersBuilder> builder) where TSchema : new()
    {
        var aiAgentParameters = new AiAgentParametersBuilder();
        builder?.Invoke(aiAgentParameters);
        return StartConversation<TSchema>(agentId, aiAgentParameters.GetParameters());
    }

    /// <summary>
    /// Continues a conversation using a fluent parameter builder.
    /// Allow to update the conversation with the new prompt or tool responses.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the conversation response.</typeparam>
    /// <param name="conversationId">The ID of the existing conversation.</param>
    /// <param name="changeVector">Optional parameter to control concurrency.</param>
    public IConversationOperations<TSchema> ResumeConversation<TSchema>(string conversationId, string changeVector = null) where TSchema : new() => new Conversation<TSchema>(this, conversationId, changeVector);
}
