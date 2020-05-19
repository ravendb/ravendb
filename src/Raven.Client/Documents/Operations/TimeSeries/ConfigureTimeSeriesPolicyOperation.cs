using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class ConfigureTimeSeriesPolicyOperation : IMaintenanceOperation<ConfigureTimeSeriesOperationResult>
    {
        private readonly string _collection;
        private readonly TimeSeriesCollectionConfiguration _config;

        public ConfigureTimeSeriesPolicyOperation(string collection, TimeSeriesCollectionConfiguration config)
        {
            _collection = collection;
            _config = config;
        }

        public RavenCommand<ConfigureTimeSeriesOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ConfigureTimeSeriesPolicyCommand(_collection, _config);
        }

        private class ConfigureTimeSeriesPolicyCommand : RavenCommand<ConfigureTimeSeriesOperationResult>, IRaftCommand
        {
            private readonly TimeSeriesCollectionConfiguration _configuration;
            private readonly string _collection;

            public ConfigureTimeSeriesPolicyCommand(string collection, TimeSeriesCollectionConfiguration configuration)
            {
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
                _collection = collection ?? throw new ArgumentNullException(nameof(collection));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/timeseries/config/policy?collection={_collection}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = ctx.ReadObject(_configuration.ToJson(),"convert time-series policy");
                        ctx.Write(stream, config);
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
}
