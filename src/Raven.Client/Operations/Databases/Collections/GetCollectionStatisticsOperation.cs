using System;
using System.Net.Http;
using Raven.Client.Commands;
using Raven.Client.Data.Collections;
using Raven.Client.Document;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Operations.Databases.Collections
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