using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.ETL;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.ConnectionStrings
{
    public class GetConnectionStringsOperation: IServerOperation<GetConnectionStringsResult> 
    {
        private readonly string _databaseName;

        public GetConnectionStringsOperation(string databaseName)
        {
            _databaseName = databaseName;
        }

        public RavenCommand<GetConnectionStringsResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetConnectionStringCommand(_databaseName);
        }

        public class GetConnectionStringCommand : RavenCommand<GetConnectionStringsResult>
        {
            private readonly string _databaseName;

            public GetConnectionStringCommand(string databaseName)
            {
                _databaseName = databaseName;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/connection-strings?name={_databaseName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.GetConnectionStringsResult(response);
            }
        }
    }

    public class GetConnectionStringsResult
    {
        public Dictionary<string, RavenConnectionString> RavenConnectionStrings { get; set; }
        public Dictionary<string, SqlConnectionString> SqlConnectionStrings { get; set; }

        public DynamicJsonValue ToJson()
        {
            var ravenConnections = new DynamicJsonValue();
            var sqlConnections = new DynamicJsonValue();

            foreach (var kvp in RavenConnectionStrings)
            {
                ravenConnections[kvp.Key] = kvp.Value.ToJson();
            }
            foreach (var kvp in SqlConnectionStrings)
            {
                sqlConnections[kvp.Key] = kvp.Value.ToJson();
            }

            return new DynamicJsonValue
            {
                [nameof(RavenConnectionStrings)] = ravenConnections,
                [nameof(SqlConnectionStrings)] = sqlConnections
            };
        }
    }
}