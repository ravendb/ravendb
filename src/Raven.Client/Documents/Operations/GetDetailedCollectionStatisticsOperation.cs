using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    /// <summary>
    /// Retrieves detailed collection statistics, providing in-depth information for each collection.
    /// This includes the count of documents, total size, and sizes of documents, revisions, and tombstones.
    /// </summary>
    public sealed class GetDetailedCollectionStatisticsOperation : IMaintenanceOperation<DetailedCollectionStatistics>
    {
        /// <inheritdoc cref="GetDetailedCollectionStatisticsOperation"/>
        public GetDetailedCollectionStatisticsOperation()
        {
        }

        public RavenCommand<DetailedCollectionStatistics> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetDetailedCollectionStatisticsCommand();
        }

        internal sealed class GetDetailedCollectionStatisticsCommand : RavenCommand<DetailedCollectionStatistics>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/collections/stats/detailed";
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<DetailedCollectionStatistics>(response);
            }
        }
    }
}
