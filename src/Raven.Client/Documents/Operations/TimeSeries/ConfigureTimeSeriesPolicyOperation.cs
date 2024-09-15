using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    /// <summary>
    /// Configure a time series policy for a specific collection.
    /// </summary>
    public class ConfigureTimeSeriesPolicyOperation : IMaintenanceOperation<ConfigureTimeSeriesOperationResult>
    {
        private readonly string _collection;
        private readonly TimeSeriesPolicy _config;

        /// <inheritdoc cref="ConfigureTimeSeriesPolicyOperation"/>
        /// <param name="collection">The name of the collection for which the time series policy is being configured.</param>
        /// <param name="config">The time series policy configuration to apply to the collection.</param>
        public ConfigureTimeSeriesPolicyOperation(string collection, TimeSeriesPolicy config)
        {
            _collection = collection;
            _config = config;
        }

        public RavenCommand<ConfigureTimeSeriesOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ConfigureTimeSeriesPolicyCommand(conventions, _collection, _config);
        }

        private sealed class ConfigureTimeSeriesPolicyCommand : RavenCommand<ConfigureTimeSeriesOperationResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly TimeSeriesPolicy _configuration;
            private readonly string _collection;

            public ConfigureTimeSeriesPolicyCommand(DocumentConventions conventions, string collection, TimeSeriesPolicy configuration)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
                _collection = collection ?? throw new ArgumentNullException(nameof(collection));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/timeseries/policy?collection={Uri.EscapeDataString(_collection)}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        var config = ctx.ReadObject(_configuration.ToJson(), "convert time-series policy");
                        await ctx.WriteAsync(stream, config).ConfigureAwait(false);
                    }, _conventions)
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

    /// <inheritdoc cref="ConfigureTimeSeriesPolicyOperation"/>
    public sealed class ConfigureRawTimeSeriesPolicyOperation : ConfigureTimeSeriesPolicyOperation
    {
        /// <inheritdoc cref="ConfigureTimeSeriesPolicyOperation(string, TimeSeriesPolicy)"/>
        public ConfigureRawTimeSeriesPolicyOperation(string collection, RawTimeSeriesPolicy config) : base(collection, config)
        {
        }
    }

    public sealed class RemoveTimeSeriesPolicyOperation : IMaintenanceOperation<ConfigureTimeSeriesOperationResult>
    {
        private readonly string _collection;
        private readonly string _name;

        public RemoveTimeSeriesPolicyOperation(string collection, string name)
        {
            _collection = collection ?? throw new ArgumentNullException(nameof(name));
            _name = name ?? throw new ArgumentNullException(nameof(collection));
        }

        public RavenCommand<ConfigureTimeSeriesOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RemoveTimeSeriesPolicyCommand(_collection, _name);
        }

        private sealed class RemoveTimeSeriesPolicyCommand : RavenCommand<ConfigureTimeSeriesOperationResult>, IRaftCommand
        {
            private readonly string _collection;
            private readonly string _name;

            public RemoveTimeSeriesPolicyCommand(string collection, string name)
            {
                _collection = collection;
                _name = name;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/timeseries/policy?collection={Uri.EscapeDataString(_collection)}&name={Uri.EscapeDataString(_name)}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Delete,
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
