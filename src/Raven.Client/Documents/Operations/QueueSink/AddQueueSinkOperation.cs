using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.QueueSink
{
    /// <summary>
    /// <para>Operation to add a queue sink configuration.</para>
    /// A queue sink task is a data integration feature that allows RavenDB to receive data from external message queue systems 
    /// such as Kafka or RabbitMQ and store it in the database.
    /// </summary>
    /// <typeparam name="T">The type of the connection string, which must inherit from <see cref="ConnectionString"/>.</typeparam>
    public class AddQueueSinkOperation<T> : IMaintenanceOperation<AddQueueSinkOperationResult> where T : ConnectionString
    {
        private readonly QueueSinkConfiguration _configuration;

        /// <inheritdoc cref="AddQueueSinkOperation{T}"/>
        /// <param name="configuration">The queue sink configuration to be added.</param>
        public AddQueueSinkOperation(QueueSinkConfiguration configuration)
        {
            _configuration = configuration;
        }

        public RavenCommand<AddQueueSinkOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new AddQueueSinkCommand(conventions, _configuration);
        }

        private class AddQueueSinkCommand : RavenCommand<AddQueueSinkOperationResult>, IRaftCommand
        {
            private readonly QueueSinkConfiguration _configuration;
            private readonly DocumentConventions _conventions;

            public AddQueueSinkCommand(DocumentConventions conventions, QueueSinkConfiguration configuration)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/queue-sink";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(
                        async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx))
                            .ConfigureAwait(false), _conventions)
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.AddQueueSinkOperationResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }

    public class AddQueueSinkOperationResult
    {
        public long RaftCommandIndex { get; set; }

        public long TaskId { get; set; }
    }
}
