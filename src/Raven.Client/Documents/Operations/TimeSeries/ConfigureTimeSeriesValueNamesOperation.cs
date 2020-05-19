using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class ConfigureTimeSeriesValueNamesOperation : IMaintenanceOperation<ConfigureTimeSeriesOperationResult>
    {
        private readonly string _collection;
        private readonly string _timeSeries;
        private readonly bool _update;
        private readonly string[] _valueNames;

        public ConfigureTimeSeriesValueNamesOperation(string collection, string timeSeries, string[] valueNames, bool update = true)
        {
            _collection = collection ?? throw new ArgumentNullException(nameof(collection));
            _timeSeries = timeSeries ?? throw new ArgumentNullException(nameof(timeSeries));
            _valueNames = valueNames ?? throw new ArgumentNullException(nameof(valueNames));

            if (valueNames.Length == 0)
                throw new ArgumentException("Must contain at least one element", nameof(valueNames));

            _update = update;
        }

        public RavenCommand<ConfigureTimeSeriesOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigureTimeSeriesValueNamesCommand(_collection, _timeSeries, _valueNames, _update);
        }

        private class ConfigureTimeSeriesValueNamesCommand : RavenCommand<ConfigureTimeSeriesOperationResult>, IRaftCommand
        {
            private readonly string _collection;
            private readonly string _timeSeries;
            private readonly string[] _valueNames;
            private readonly bool _update;

            public ConfigureTimeSeriesValueNamesCommand(string collection, string timeSeries, string[] valueNames, bool update)
            {
                _collection = collection;
                _timeSeries = timeSeries;
                _valueNames = valueNames;
                _update = update;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/timeseries/config/names";
                var parameters = new Parameters
                {
                    Collection = _collection,
                    TimeSeries = _timeSeries,
                    ValueNames = _valueNames,
                    Update = _update
                };

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = ctx.ReadObject(parameters.ToJson(),"convert time-series configuration");
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

        public class Parameters : IDynamicJson
        {
            public string Collection;
            public string TimeSeries;
            public string[] ValueNames;
            public bool Update;

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
