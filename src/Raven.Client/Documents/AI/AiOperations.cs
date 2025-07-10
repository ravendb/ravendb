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
    /// Create starts a new chat with an AI agent using a dictionary of parameters.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat response.</typeparam>
    /// <param name="agentId">The ID of the AI agent to chat with.</param>
    /// <param name="parameters">Required chat parameters.</param>
    public IChatOperations<TSchema> CreateChat<TSchema>(string agentId, Dictionary<string, object> parameters = null) where TSchema : new() => new Chat<TSchema>(this, agentId, parameters);
    
    /// <summary>
    /// Create a new chat with an AI agent using a dictionary of parameters.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat response.</typeparam>
    /// <param name="agentId">The ID of the AI agent to chat with.</param>
    /// <param name="builder">A builder to define the required chat parameters.</param>
    public IChatOperations<TSchema> CreateChat<TSchema>(string agentId, Action<IAiAgentParametersBuilder> builder) where TSchema : new()
    {
        var aiAgentParameters = new AiAgentParametersBuilder();
        builder?.Invoke(aiAgentParameters);
        return CreateChat<TSchema>(agentId, aiAgentParameters.GetParameters());
    }

    /// <summary>
    /// Continues a chat using a fluent parameter builder.
    /// Allow to update the chat with the new prompt or tool responses.
    /// </summary>
    /// <typeparam name="TSchema">The schema type for the chat response.</typeparam>
    /// <param name="chatId">The ID of the existing chat.</param>
    public IChatOperations<TSchema> ResumeChat<TSchema>(string chatId) where TSchema : new() => new Chat<TSchema>(this, chatId);
}
