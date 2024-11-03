using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations
{
    /// <summary>
    /// Retrieves collection revisions statistics, providing in-depth information for each collection.
    /// This includes the count of all revisions, and revisions count for each collection.
    /// </summary>
    public sealed class GetCollectionRevisionsStatisticsOperation : IMaintenanceOperation<CollectionRevisionsStatistics>
    {
        /// <inheritdoc cref="GetCollectionRevisionsStatisticsOperation"/>
        public GetCollectionRevisionsStatisticsOperation()
        {
        }

        public RavenCommand<CollectionRevisionsStatistics> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetCollectionRevisionsStatisticsCommand();
        }

        internal sealed class GetCollectionRevisionsStatisticsCommand : RavenCommand<CollectionRevisionsStatistics>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/collections/stats/revisions";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<CollectionRevisionsStatistics>(response);
            }
        }
    }

    public class CollectionRevisionsStatistics
    {
        public long CountOfRevisions { get; set; }
        public Dictionary<string, long> Collections { get; set; }

        public DynamicJsonValue ToJson()
        {
            DynamicJsonValue collections = new DynamicJsonValue();
            foreach (var collection in Collections)
            {
                collections[collection.Key] = collection.Value;
            }

            return new DynamicJsonValue()
            {
                [nameof(CollectionRevisionsStatistics.CountOfRevisions)] = CountOfRevisions,
                [nameof(CollectionRevisionsStatistics.Collections)] = collections
            };
        }
    }
}
