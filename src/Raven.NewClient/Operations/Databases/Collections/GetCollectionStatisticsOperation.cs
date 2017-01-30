using System;
using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data.Collections;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Operations;
using Sparrow.Json;

namespace Raven.NewClient.Client.Operations.Databases.Collections
{
    public class GetCollectionStatisticsOperation : IAdminOperation<CollectionStatistics>
    {
        public RavenCommand<CollectionStatistics> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new GetCollectionStatisticsCommand(conventions);
        }

        private class GetCollectionStatisticsCommand : RavenCommand<CollectionStatistics>
        {
            private readonly DocumentConvention _conventions;

            public GetCollectionStatisticsCommand(DocumentConvention conventions)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));

                _conventions = conventions;
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