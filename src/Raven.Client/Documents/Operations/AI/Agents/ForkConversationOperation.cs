using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// Creates a new conversation by forking from a previously captured snapshot token.
/// The forked conversation contains all messages and state up to the point where the
/// snapshot was taken, and receives a new document ID.
///
/// <para>
/// Sub-conversation documents (including nested sub-conversations) are also forked from their
/// revisions and linked to the new parent with adjusted IDs. Existing history documents are
/// shared (not duplicated) between the original and forked conversations.
/// </para>
///
/// <para>
/// If <c>newConversationId</c> resolves to an ID where a conversation document
/// already exists (whether the original conversation or a different one), the existing document
/// is overwritten with the forked state. Any sub-conversations tracked by the overwritten
/// document that do not exist in the fork are deleted.
/// </para>
///
/// <para>
/// <strong>Important:</strong> This operation requires that the revisions referenced by the
/// snapshot token still exist. If they have been purged by the revisions retention policy
/// or by <see cref="AiOperations.PurgeConversationSnapshotsAsync"/>, this operation will
/// fail with an error identifying the missing revision.
/// </para>
/// </summary>
public class ForkConversationOperation : IMaintenanceOperation<AiForkConversationResult>
{
    private readonly string _snapshotToken;
    private readonly string _newConversationId;
    private readonly string _expectedChangeVector;

    public ForkConversationOperation(string snapshotToken, string newConversationId = null, string expectedChangeVector = null)
    {
        ValidationMethods.AssertNotNullOrEmpty(snapshotToken, nameof(snapshotToken));
        _snapshotToken = snapshotToken;
        _newConversationId = newConversationId;
        _expectedChangeVector = expectedChangeVector;
    }

    public RavenCommand<AiForkConversationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new ForkConversationCommand(_snapshotToken, _newConversationId, _expectedChangeVector, conventions);
    }

    private sealed class ForkConversationCommand : RavenCommand<AiForkConversationResult>, IRaftCommand
    {
        private readonly string _snapshotToken;
        private readonly string _newConversationId;
        private readonly string _expectedChangeVector;
        private readonly DocumentConventions _conventions;

        public ForkConversationCommand(string snapshotToken, string newConversationId, string expectedChangeVector, DocumentConventions conventions)
        {
            _snapshotToken = snapshotToken;
            _newConversationId = newConversationId;
            _expectedChangeVector = expectedChangeVector;
            _conventions = conventions;

            if (_newConversationId?.EndsWith("|") == true)
            {
                _raftId = Guid.NewGuid().ToString();
            }
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/agent/fork";

            var body = new DynamicJsonValue
            {
                ["SnapshotToken"] = _snapshotToken,
                ["NewConversationId"] = _newConversationId,
                ["ExpectedChangeVector"] = _expectedChangeVector
            };

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await ctx.WriteAsync(stream, ctx.ReadObject(body, "fork-conversation")).ConfigureAwait(false);
                }, _conventions)
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = AiForkConversationResult.Convert(response);
        }

        private string _raftId = string.Empty;
        public string RaftUniqueRequestId => _raftId;
    }
}
