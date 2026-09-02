using System;
using System.Collections.Generic;
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

    /// <summary>
    /// Creates a new conversation by forking from a previously captured snapshot token.
    /// The forked conversation contains all messages and state up to the point where the
    /// snapshot was taken.
    ///
    /// <para>
    /// Sub-conversation documents (including nested sub-conversations) are forked from their
    /// revisions and linked to the new parent. Existing history documents are shared between
    /// the original and forked conversations.
    /// </para>
    ///
    /// <para>
    /// If <paramref name="newConversationId"/> resolves to an ID that already exists (whether
    /// the original conversation or a different one), the existing document is overwritten
    /// with the forked state. Any sub-conversations tracked by the overwritten document that
    /// do not exist in the fork are deleted.
    /// </para>
    ///
    /// <para>
    /// <strong>Important:</strong> This operation requires that the revisions referenced by the
    /// snapshot token still exist. If they have been purged by the revisions retention policy
    /// or by <see cref="PurgeConversationSnapshotsAsync"/>, this operation will fail.
    /// </para>
    /// </summary>
    /// <param name="snapshotToken">
    /// The opaque token obtained from <see cref="AiAnswer{TAnswer}.SnapshotToken"/>
    /// or <see cref="GetConversationSnapshotsAsync"/>.
    /// </param>
    /// <param name="newConversationId">
    /// The document ID for the forked conversation. Follows the same conventions as
    /// <see cref="Conversation"/>:
    /// <list type="bullet">
    ///   <item>An explicit ID (e.g. <c>"chats/42"</c>) creates a document with that exact ID.</item>
    ///   <item>A prefix ending with <c>"/"</c> (e.g. <c>"chats/"</c>) auto-generates a numeric suffix.</item>
    ///   <item>A prefix ending with <c>"|"</c> (e.g. <c>"chats|"</c>) uses cluster-wide identity generation.</item>
    ///   <item>When <c>null</c>, the server generates a GUID-based ID.</item>
    /// </list>
    /// </param>
    /// <param name="expectedChangeVector">
    /// Optional concurrency guard for the target conversation document. When non-null, the server
    /// verifies it against the existing document's change vector before overwriting. Pass an empty
    /// string to assert that the target document does not yet exist. This check applies only to the
    /// root conversation document, not to its sub-conversations.
    /// </param>
    /// <param name="token">A cancellation token.</param>
    /// <returns>
    /// An <see cref="AiForkConversationResult"/> containing the forked conversation's ID
    /// and change vector. Use these to open the forked conversation via <see cref="Conversation"/>.
    /// </returns>
    public Task<AiForkConversationResult> ForkConversationAsync(
        string snapshotToken,
        string newConversationId = null,
        string expectedChangeVector = null,
        CancellationToken token = default)
    {
        return _executor.SendAsync(new ForkConversationOperation(snapshotToken, newConversationId, expectedChangeVector), token);
    }

    /// <inheritdoc cref="ForkConversationAsync"/>
    public AiForkConversationResult ForkConversation(
        string snapshotToken,
        string newConversationId = null,
        string expectedChangeVector = null)
    {
        return AsyncHelpers.RunSync(() => ForkConversationAsync(snapshotToken, newConversationId, expectedChangeVector));
    }

    /// <summary>
    /// Creates a snapshot of the current conversation state without running a conversation turn.
    /// Returns a snapshot token that can be passed to <see cref="ForkConversationAsync"/>.
    ///
    /// <para>
    /// This is useful when you want to capture the current state for potential forking
    /// without sending a new prompt to the model.
    /// </para>
    ///
    /// <para>
    /// If the conversation has pending actions (open tool calls awaiting user responses),
    /// they remain open in the forked conversation and need to be resolved separately.
    /// </para>
    /// </summary>
    /// <param name="conversationId">The conversation document ID to snapshot.</param>
    /// <param name="token">A cancellation token.</param>
    /// <returns>
    /// An <see cref="AiConversationSnapshot"/> containing the token and creation timestamp.
    /// </returns>
    /// <exception cref="InvalidOperationException">Thrown when the conversation does not exist.</exception>
    public Task<AiConversationSnapshot> CreateSnapshotAsync(string conversationId, CancellationToken token = default)
    {
        return _executor.SendAsync(new CreateConversationSnapshotOperation(conversationId), token);
    }

    /// <inheritdoc cref="CreateSnapshotAsync"/>
    public AiConversationSnapshot CreateSnapshot(string conversationId)
    {
        return AsyncHelpers.RunSync(() => CreateSnapshotAsync(conversationId));
    }

    /// <summary>
    /// Returns available snapshots for a conversation, ordered by creation time (newest first).
    /// Each entry contains a snapshot token and the date it was captured.
    /// Supports cursor-based paging via <paramref name="before"/>.
    ///
    /// <para>
    /// Only snapshots whose revisions still exist are returned. Snapshots whose revisions
    /// have been purged by the retention policy are excluded.
    /// </para>
    /// </summary>
    /// <param name="conversationId">The conversation document ID.</param>
    /// <param name="before">
    /// When specified, only returns snapshots created before this date (exclusive).
    /// Use the <see cref="AiConversationSnapshot.CreatedAt"/> of the last item in a previous
    /// page to fetch the next page of older snapshots.
    /// When <c>null</c>, returns the most recent snapshots.
    /// </param>
    /// <param name="pageSize">Maximum number of snapshots to return. Default is 25.</param>
    /// <param name="token">A cancellation token.</param>
    /// <returns>A list of available snapshots, or an empty list if none exist.</returns>
    public Task<List<AiConversationSnapshot>> GetConversationSnapshotsAsync(
        string conversationId,
        DateTime? before = null,
        int pageSize = 25,
        CancellationToken token = default)
    {
        return _executor.SendAsync(new GetConversationSnapshotsOperation(conversationId, before, pageSize), token);
    }

    /// <inheritdoc cref="GetConversationSnapshotsAsync"/>
    public List<AiConversationSnapshot> GetConversationSnapshots(
        string conversationId,
        DateTime? before = null,
        int pageSize = 25)
    {
        return AsyncHelpers.RunSync(() => GetConversationSnapshotsAsync(conversationId, before, pageSize));
    }

    /// <summary>
    /// Deletes all snapshots for the specified conversation,
    /// invalidating any outstanding snapshot tokens. The conversation itself is not affected.
    ///
    /// <para>
    /// Use this when the conversation has no more need to fork its snapshots,
    /// or to reclaim storage used by accumulated snapshots.
    /// </para>
    /// </summary>
    /// <param name="conversationId">The conversation whose snapshots should be purged.</param>
    /// <param name="token">A cancellation token.</param>
    public Task PurgeConversationSnapshotsAsync(string conversationId, CancellationToken token = default)
    {
        return _executor.SendAsync(new PurgeConversationSnapshotsOperation(conversationId), token);
    }

    /// <summary>
    /// Deletes all snapshots for the specified conversation that were created strictly before the given date,
    /// invalidating the corresponding snapshot tokens. The conversation itself is not affected.
    /// </summary>
    /// <param name="conversationId">The conversation whose snapshots should be purged.</param>
    /// <param name="before">Only snapshots created strictly before this date (UTC, exclusive) will be deleted.</param>
    /// <param name="token">A cancellation token.</param>
    public Task PurgeConversationSnapshotsAsync(string conversationId, DateTime before, CancellationToken token = default)
    {
        return _executor.SendAsync(new PurgeConversationSnapshotsOperation(conversationId, before), token);
    }

    /// <inheritdoc cref="PurgeConversationSnapshotsAsync(string, CancellationToken)"/>
    public void PurgeConversationSnapshots(string conversationId)
    {
        AsyncHelpers.RunSync(() => PurgeConversationSnapshotsAsync(conversationId));
    }

    /// <inheritdoc cref="PurgeConversationSnapshotsAsync(string, DateTime, CancellationToken)"/>
    public void PurgeConversationSnapshots(string conversationId, DateTime before)
    {
        AsyncHelpers.RunSync(() => PurgeConversationSnapshotsAsync(conversationId, before));
    }
}
