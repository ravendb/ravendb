using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.ConnectionStrings
{
    /// <summary>
    /// Operation to create or update a server-wide connection string.
    /// The connection string will be automatically propagated to all databases in the cluster
    /// (unless explicitly excluded via <see cref="ServerWideConnectionString.ExcludedDatabases"/>).
    /// </summary>
    public sealed class PutServerWideConnectionStringOperation : IServerOperation<PutServerWideConnectionStringResult>
    {
        private readonly ServerWideConnectionString _connectionString;

        /// <inheritdoc cref="PutServerWideConnectionStringOperation"/>
        /// <param name="connectionString">The server-wide connection string to create or update.</param>
        public PutServerWideConnectionStringOperation(ServerWideConnectionString connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            if (connectionString.ConnectionString == null)
                throw new ArgumentNullException(nameof(connectionString), $"{nameof(ServerWideConnectionString.ConnectionString)} must not be null.");
        }

        public RavenCommand<PutServerWideConnectionStringResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutServerWideConnectionStringCommand(conventions, context, _connectionString);
        }

        private sealed class PutServerWideConnectionStringCommand : RavenCommand<PutServerWideConnectionStringResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly BlittableJsonReaderObject _connectionString;

            public PutServerWideConnectionStringCommand(DocumentConventions conventions, JsonOperationContext context, ServerWideConnectionString connectionString)
            {
                if (connectionString == null)
                    throw new ArgumentNullException(nameof(connectionString));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));

                _connectionString = context.ReadObject(connectionString.ToJson(), "server-wide-connection-string");
            }

            public override bool IsReadRequest => false;

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/connection-strings";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _connectionString).ConfigureAwait(false), _conventions)
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.PutServerWideConnectionStringResult(response);
            }
        }
    }

    /// <summary>
    /// The result of a <see cref="PutServerWideConnectionStringOperation"/>.
    /// </summary>
    public sealed class PutServerWideConnectionStringResult
    {
        /// <summary>
        /// The Raft command index assigned to this operation. Can be used to wait for the operation to be applied across the cluster.
        /// </summary>
        public long RaftCommandIndex { get; set; }
    }
}
