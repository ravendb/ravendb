using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class ConfigureTimeSeriesValueNamesOperation : IMaintenanceOperation<ConfigureTimeSeriesOperationResult>
    {
        private readonly Parameters _parameters;

        public ConfigureTimeSeriesValueNamesOperation(Parameters parameters)
        {
            _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
            _parameters.Validate();
        }

        public RavenCommand<ConfigureTimeSeriesOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigureTimeSeriesValueNamesCommand(_parameters);
        }

        private class ConfigureTimeSeriesValueNamesCommand : RavenCommand<ConfigureTimeSeriesOperationResult>, IRaftCommand
        {
            private readonly Parameters _parameters;

            public ConfigureTimeSeriesValueNamesCommand(Parameters parameters)
            {
                _parameters = parameters;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/timeseries/names/config";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        var config = ctx.ReadObject(_parameters.ToJson(), "convert time-series configuration");
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

        public class Parameters : IDynamicJson
        {
            public string Collection;
            public string TimeSeries;
            public string[] ValueNames;
            public bool Update;

            internal void Validate()
            {
                if (string.IsNullOrEmpty(Collection))
                    throw new ArgumentNullException(nameof(Collection));
                if (string.IsNullOrEmpty(TimeSeries))
                    throw new ArgumentNullException(nameof(TimeSeries));
                if (ValueNames == null || ValueNames.Length == 0)
                    throw new ArgumentException($"{nameof(ValueNames)} can't be empty.");
            }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Collection)] = Collection,
                    [nameof(TimeSeries)] = TimeSeries,
                    [nameof(ValueNames)] = new DynamicJsonArray(ValueNames),
                    [nameof(Update)] = Update
                };
            }
        }
    }
}
