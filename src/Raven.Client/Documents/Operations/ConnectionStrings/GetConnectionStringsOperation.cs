using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ConnectionStrings
{
    public class GetConnectionStringsOperation: IMaintenanceOperation<GetConnectionStringsResult> 
    {
        private readonly string _connectionStringName;

        private readonly ConnectionStringType _type;

        public GetConnectionStringsOperation(string connectionStringName, ConnectionStringType type)
        {
            _connectionStringName = connectionStringName;
            _type = type;
        }

        public GetConnectionStringsOperation()
        {
            // get them all
        }

        public RavenCommand<GetConnectionStringsResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new GetConnectionStringCommand(_connectionStringName, _type);
        }

        private class GetConnectionStringCommand : RavenCommand<GetConnectionStringsResult>
        {
            private readonly string _connectionStringName;

            private readonly ConnectionStringType _type;


            public GetConnectionStringCommand(string connectionStringName = null, ConnectionStringType type = ConnectionStringType.None)
            {
                _connectionStringName = connectionStringName;
                _type = type;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/connection-strings";
                if (_connectionStringName != null)
                {
                    url += $"?connectionStringName={Uri.EscapeDataString(_connectionStringName)}&type={_type}";
                }

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
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
        public Dictionary<string, OlapConnectionString> OlapConnectionStrings { get; set; }

        public DynamicJsonValue ToJson()
        {
            var ravenConnections = new DynamicJsonValue();
            var sqlConnections = new DynamicJsonValue();
            var olapConnections = new DynamicJsonValue();

            foreach (var kvp in RavenConnectionStrings)
            {
                ravenConnections[kvp.Key] = kvp.Value.ToJson();
            }
            foreach (var kvp in SqlConnectionStrings)
            {
                sqlConnections[kvp.Key] = kvp.Value.ToJson();
            }
            foreach (var kvp in OlapConnectionStrings)
            {
                olapConnections[kvp.Key] = kvp.Value.ToJson();
            }

            return new DynamicJsonValue
            {
                [nameof(RavenConnectionStrings)] = ravenConnections,
                [nameof(SqlConnectionStrings)] = sqlConnections,
                [nameof(OlapConnectionStrings)] = olapConnections
            };
        }
    }
}
