using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    internal class GetTimeSeriesConfigurationOperation : IMaintenanceOperation<TimeSeriesConfiguration>
    {
        public RavenCommand<TimeSeriesConfiguration> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
           return new GetTimeSeriesConfigurationCommand();
        }

        internal class GetTimeSeriesConfigurationCommand : RavenCommand<TimeSeriesConfiguration>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/timeseries/config";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.TimeSeriesConfiguration(response);
            }
        }
    }
}
