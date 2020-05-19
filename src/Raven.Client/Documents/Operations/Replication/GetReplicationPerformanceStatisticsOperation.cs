using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Replication
{
    public class GetReplicationPerformanceStatisticsOperation : IMaintenanceOperation<ReplicationPerformance>
    {
        public RavenCommand<ReplicationPerformance> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetReplicationPerformanceStatisticsCommand(conventions);
        }

        private class GetReplicationPerformanceStatisticsCommand : RavenCommand<ReplicationPerformance>
        {
            private readonly DocumentConventions _conventions;

            public GetReplicationPerformanceStatisticsCommand(DocumentConventions conventions)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/replication/performance";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = _conventions.Serialization.DefaultConverter.FromBlittable<ReplicationPerformance>(response, "replication/performance");
            }
        }
    }
}
