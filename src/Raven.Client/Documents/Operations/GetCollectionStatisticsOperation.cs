using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetCollectionStatisticsOperation : IAdminOperation<CollectionStatistics>
    {
        public RavenCommand<CollectionStatistics> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetCollectionStatisticsCommand(conventions);
        }

        private class GetCollectionStatisticsCommand : RavenCommand<CollectionStatistics>
        {
            private readonly DocumentConventions _conventions;

            public GetCollectionStatisticsCommand(DocumentConventions conventions)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/collections/stats";
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = (CollectionStatistics)_conventions.DeserializeEntityFromBlittable(typeof(CollectionStatistics), response);
            }
        }
    }
}