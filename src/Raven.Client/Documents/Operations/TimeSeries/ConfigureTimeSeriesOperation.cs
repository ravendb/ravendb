using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class ConfigureTimeSeriesOperation : IMaintenanceOperation<ConfigureTimeSeriesOperationResult>
    {
        private readonly TimeSeriesConfiguration _configuration;

        public ConfigureTimeSeriesOperation(TimeSeriesConfiguration configuration)
        {
            _configuration = configuration ?? new TimeSeriesConfiguration();
        }

        public RavenCommand<ConfigureTimeSeriesOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigureTimeSeriesCommand(_configuration);
        }

        private class ConfigureTimeSeriesCommand : RavenCommand<ConfigureTimeSeriesOperationResult>, IRaftCommand
        {
            private readonly TimeSeriesConfiguration _configuration;

            public ConfigureTimeSeriesCommand(TimeSeriesConfiguration configuration)
            {
                _configuration = configuration;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/timeseries/config";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        var config = ctx.ReadObject(_configuration.ToJson(), "convert time-series configuration");
                        await ctx.WriteAsync(stream, config).ConfigureAwait(false);
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ConfigureTimeSeriesOperationResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }

    public class ConfigureTimeSeriesOperationResult
    {
        public long? RaftCommandIndex { get; set; }
    }
}
