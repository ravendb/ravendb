using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.ConnectionStrings
{
    /// <summary>
    /// Operation to remove a server-wide connection string from the cluster.
    /// The connection string will also be removed from all database records that received it.
    /// The operation will fail if the connection string is currently in use by any ongoing task.
    /// </summary>
    /// <typeparam name="T">The type of the connection string to remove (e.g., <c>RavenConnectionString</c>, <c>SqlConnectionString</c>).</typeparam>
    public sealed class RemoveServerWideConnectionStringOperation<T> : IServerOperation<RemoveServerWideConnectionStringResult> where T : ConnectionString
    {
        private readonly T _connectionString;

        /// <inheritdoc cref="RemoveServerWideConnectionStringOperation{T}"/>
        /// <param name="connectionString">The connection string to remove. Only the <see cref="ConnectionString.Name"/> property is required.</param>
        public RemoveServerWideConnectionStringOperation(T connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            if (string.IsNullOrWhiteSpace(connectionString.Name))
                throw new ArgumentException("Connection string name must not be null or empty.", nameof(connectionString));
        }

        public RavenCommand<RemoveServerWideConnectionStringResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RemoveServerWideConnectionStringCommand(_connectionString);
        }

        private sealed class RemoveServerWideConnectionStringCommand : RavenCommand<RemoveServerWideConnectionStringResult>, IRaftCommand
        {
            private readonly T _connectionString;

            public RemoveServerWideConnectionStringCommand(T connectionString)
            {
                _connectionString = connectionString;
            }

            public override bool IsReadRequest => false;

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/connection-strings?connectionString={Uri.EscapeDataString(_connectionString.Name)}&type={_connectionString.Type}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.RemoveServerWideConnectionStringResult(response);
            }
        }
    }

    /// <summary>
    /// The result of a <see cref="RemoveServerWideConnectionStringOperation{T}"/>.
    /// </summary>
    public sealed class RemoveServerWideConnectionStringResult
    {
        /// <summary>
        /// The Raft command index assigned to this operation. Can be used to wait for the operation to be applied across the cluster.
        /// </summary>
        public long RaftCommandIndex { get; set; }
    }
}
