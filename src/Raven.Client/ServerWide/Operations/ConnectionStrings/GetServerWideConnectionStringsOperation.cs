using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.ConnectionStrings
{
    public sealed class GetServerWideConnectionStringsOperation : IServerOperation<GetServerWideConnectionStringsResult>
    {
        private readonly string _connectionStringName;
        private readonly ConnectionStringType _type;

        public GetServerWideConnectionStringsOperation(string connectionStringName, ConnectionStringType type)
        {
            _connectionStringName = connectionStringName;
            _type = type;
        }

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
                    queryParams.Add($"name={Uri.EscapeDataString(_connectionStringName)}");
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

    public sealed class GetServerWideConnectionStringsResult : IDynamicJson
    {
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
