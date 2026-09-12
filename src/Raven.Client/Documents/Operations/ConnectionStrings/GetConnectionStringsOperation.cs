using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ConnectionStrings
{
    /// <summary>
    /// Operation to retrieve connection strings from the database.
    /// </summary>
    public sealed class GetConnectionStringsOperation: IMaintenanceOperation<GetConnectionStringsResult> 
    {
        private readonly string _connectionStringName;

        private readonly ConnectionStringType _type;

        /// <inheritdoc cref="GetConnectionStringsOperation"/>
        /// <param name="connectionStringName">The name of the connection string to retrieve.</param>
        /// <param name="type">The type of the connection string.</param>
        public GetConnectionStringsOperation(string connectionStringName, ConnectionStringType type)
        {
            _connectionStringName = connectionStringName;
            _type = type;
        }

        /// <inheritdoc cref="GetConnectionStringsOperation"/>
        public GetConnectionStringsOperation()
        {
            // get them all
        }

        public RavenCommand<GetConnectionStringsResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new GetConnectionStringCommand(_connectionStringName, _type);
        }

        private sealed class GetConnectionStringCommand : RavenCommand<GetConnectionStringsResult>
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

    public sealed class GetConnectionStringsResult
    {
        public Dictionary<string, RavenConnectionString> RavenConnectionStrings { get; set; }
        public Dictionary<string, SqlConnectionString> SqlConnectionStrings { get; set; }
        public Dictionary<string, OlapConnectionString> OlapConnectionStrings { get; set; }
        public Dictionary<string, ElasticSearchConnectionString> ElasticSearchConnectionStrings { get; set; }
        public Dictionary<string, QueueConnectionString> QueueConnectionStrings { get; set; }
        public Dictionary<string, SnowflakeConnectionString> SnowflakeConnectionStrings { get; set; }
        public Dictionary<string, AiConnectionString> AiConnectionStrings { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            var result = new DynamicJsonValue();

            AddConnections(RavenConnectionStrings, nameof(RavenConnectionStrings));
            AddConnections(SqlConnectionStrings, nameof(SqlConnectionStrings));
            AddConnections(OlapConnectionStrings, nameof(OlapConnectionStrings));
            AddConnections(ElasticSearchConnectionStrings, nameof(ElasticSearchConnectionStrings));
            AddConnections(QueueConnectionStrings, nameof(QueueConnectionStrings));
            AddConnections(SnowflakeConnectionStrings, nameof(SnowflakeConnectionStrings));
            AddConnections(AiConnectionStrings, nameof(AiConnectionStrings));

            return result;

            void AddConnections<T>(Dictionary<string, T> connectionStrings, string propertyName)
                where T : ConnectionString
            {
                if (connectionStrings == null)
                {
                    result[propertyName] = null;
                    return;
                }

                var jsonConnections = new DynamicJsonValue();

                foreach (var kvp in connectionStrings)
                    jsonConnections[kvp.Key] = kvp.Value.ToStudioJson();

                result[propertyName] = jsonConnections;
            }
        }
    }
}
