using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Http;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.ConnectionStrings
{
    /// <summary>
    /// Operation to retrieve server-wide connection strings from the cluster.
    /// Can retrieve all server-wide connection strings or filter by name and/or type.
    /// </summary>
    public sealed class GetServerWideConnectionStringsOperation : IServerOperation<GetServerWideConnectionStringsResult>
    {
        private readonly string _connectionStringName;
        private readonly ConnectionStringType _type;

        /// <inheritdoc cref="GetServerWideConnectionStringsOperation"/>
        /// <param name="connectionStringName">The name of a specific connection string to retrieve.</param>
        /// <param name="type">The type of connection strings to retrieve.</param>
        public GetServerWideConnectionStringsOperation(string connectionStringName, ConnectionStringType type)
        {
            if (string.IsNullOrWhiteSpace(connectionStringName))
                throw new ArgumentException("Connection string name must not be null or empty.", nameof(connectionStringName));

            _connectionStringName = connectionStringName;
            _type = type;
        }

        /// <summary>
        /// Retrieves all server-wide connection strings of all types.
        /// </summary>
        public GetServerWideConnectionStringsOperation()
        {
            // get them all
        }

        public RavenCommand<GetServerWideConnectionStringsResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetServerWideConnectionStringsCommand(_connectionStringName, _type);
        }

        private sealed class GetServerWideConnectionStringsCommand : RavenCommand<GetServerWideConnectionStringsResult>
        {
            private readonly string _connectionStringName;
            private readonly ConnectionStringType _type;

            public GetServerWideConnectionStringsCommand(string connectionStringName = null, ConnectionStringType type = ConnectionStringType.None)
            {
                _connectionStringName = connectionStringName;
                _type = type;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/connection-strings";

                var queryParams = new List<string>();
                if (_connectionStringName != null)
                    queryParams.Add($"connectionStringName={Uri.EscapeDataString(_connectionStringName)}");
                if (_type != ConnectionStringType.None)
                    queryParams.Add($"type={_type}");

                if (queryParams.Count > 0)
                    url += $"?{string.Join("&", queryParams)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = GetServerWideConnectionStringsResult.FromBlittable(response);
            }
        }
    }

    /// <summary>
    /// The result of a <see cref="GetServerWideConnectionStringsOperation"/>.
    /// </summary>
    public sealed class GetServerWideConnectionStringsResult : IDynamicJson
    {
        /// <summary>
        /// The list of server-wide connection strings matching the query criteria.
        /// </summary>
        public List<ServerWideConnectionString> Results { get; set; } = new List<ServerWideConnectionString>();

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Results)] = new DynamicJsonArray(Results.Select(x => x.ToJson()))
            };
        }

        internal static GetServerWideConnectionStringsResult FromBlittable(BlittableJsonReaderObject blittable)
        {
            var result = new GetServerWideConnectionStringsResult();

            if (blittable.TryGet(nameof(Results), out BlittableJsonReaderArray array) && array != null)
            {
                foreach (BlittableJsonReaderObject item in array)
                {
                    result.Results.Add(ServerWideConnectionString.FromBlittable(item));
                }
            }

            return result;
        }
    }

}
