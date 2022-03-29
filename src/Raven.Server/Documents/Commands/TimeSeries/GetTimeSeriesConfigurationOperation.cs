using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Http;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.TimeSeries
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

                Result = JsonDeserializationServer.TimeSeriesConfiguration(response);
            }
        }
    }
}
