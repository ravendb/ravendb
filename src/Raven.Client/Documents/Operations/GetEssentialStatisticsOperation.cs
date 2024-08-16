using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations;

/// <summary>
/// Retrieves essential database statistics, focusing on critical metrics such as
/// the number of documents and document extensions, and essential index information.
/// </summary>
public sealed class GetEssentialStatisticsOperation : IMaintenanceOperation<EssentialDatabaseStatistics>
{
    private readonly string _debugTag;

    /// <inheritdoc cref="GetEssentialStatisticsOperation"/>
    public GetEssentialStatisticsOperation()
    {
    }

    /// <inheritdoc cref="GetEssentialStatisticsOperation"/>
    /// <param name="debugTag">An optional tag for enhanced logging or debugging purposes.</param>
    internal GetEssentialStatisticsOperation(string debugTag)
    {
        _debugTag = debugTag;
    }

    public RavenCommand<EssentialDatabaseStatistics> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new GetEssentialStatisticsCommand(_debugTag);
    }

    internal sealed class GetEssentialStatisticsCommand : RavenCommand<EssentialDatabaseStatistics>
    {
        private readonly string _debugTag;

        public GetEssentialStatisticsCommand(string debugTag)
            : this(debugTag, nodeTag: null)
        {
        }

        internal GetEssentialStatisticsCommand(string debugTag, string nodeTag)
        {
            _debugTag = debugTag;
            SelectedNodeTag = nodeTag;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/stats/essential";
            if (_debugTag != null)
                url += "?" + _debugTag;

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.GetEssentialDatabaseStatistics(response);
        }

        public override bool IsReadRequest => true;
    }
}
