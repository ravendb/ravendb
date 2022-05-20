using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations;

public class GetBasicStatisticsOperation : IMaintenanceOperation<BasicDatabaseStatistics>
{
    private readonly string _debugTag;

    public GetBasicStatisticsOperation()
    {
    }

    internal GetBasicStatisticsOperation(string debugTag)
    {
        _debugTag = debugTag;
    }

    public RavenCommand<BasicDatabaseStatistics> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new GetBasicStatisticsCommand(_debugTag);
    }

    internal class GetBasicStatisticsCommand : RavenCommand<BasicDatabaseStatistics>
    {
        private readonly string _debugTag;

        public GetBasicStatisticsCommand(string debugTag)
            : this(debugTag, nodeTag: null)
        {
        }

        internal GetBasicStatisticsCommand(string debugTag, string nodeTag)
        {
            _debugTag = debugTag;
            SelectedNodeTag = nodeTag;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/stats/basic";
            if (_debugTag != null)
                url += "?" + _debugTag;

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.GetBasicDatabaseStatistics(response);
        }

        public override bool IsReadRequest => true;
    }
}
