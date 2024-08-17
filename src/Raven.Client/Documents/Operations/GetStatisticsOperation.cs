using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    /// <summary>
    /// Retrieves database statistics. This operation provides various metrics about the database, such as the database change vector,
    /// the number of documents, indexes, collections, and other relevant details.
    /// </summary>
    public sealed class GetStatisticsOperation : IMaintenanceOperation<DatabaseStatistics>
    {
        private readonly string _debugTag;
        private readonly string _nodeTag;

        /// <inheritdoc cref="GetStatisticsOperation"/>
        public GetStatisticsOperation()
        {
        }

        /// <inheritdoc cref = "GetStatisticsOperation" />
        /// <param name="debugTag">An optional tag for enhanced logging or debugging purposes.</param>
        /// <param name="nodeTag">An optional node tag to target a specific server node.</param>
        internal GetStatisticsOperation(string debugTag, string nodeTag = null)
        {
            _debugTag = debugTag;
            _nodeTag = nodeTag;
        }

        public RavenCommand<DatabaseStatistics> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetStatisticsCommand(_debugTag, _nodeTag);
        }

        internal sealed class GetStatisticsCommand : RavenCommand<DatabaseStatistics>
        {
            private readonly string _debugTag;

            public GetStatisticsCommand(string debugTag, string nodeTag)
            {
                _debugTag = debugTag;
                SelectedNodeTag = nodeTag;
                Timeout = TimeSpan.FromSeconds(15);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/stats";
                if (_debugTag != null)
                {
                    url += "?" + _debugTag;
                }

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.GetStatisticsResult(response);
            }

            public override bool IsReadRequest => true;
        }
    }
}
