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
    /// <para>Operation to update an existing queue sink configuration.</para>
    /// A queue sink task is a data integration feature that allows RavenDB to receive data from external message queue systems 
    /// such as Kafka or RabbitMQ and store it in the database.
    /// </summary>
    /// <typeparam name="T">The type of the connection string, which must inherit from <see cref="ConnectionString"/>.</typeparam>
    public class UpdateQueueSinkOperation<T> : IMaintenanceOperation<UpdateQueueSinkOperationResult> where T : ConnectionString
    {
        private readonly long _taskId;
        private readonly QueueSinkConfiguration _configuration;

        /// <inheritdoc cref="UpdateQueueSinkOperation{T}"/>
        /// <param name="taskId">The unique identifier of the queue sink task to be updated.</param>
        /// <param name="configuration">The updated queue sink configuration.</param>
        public UpdateQueueSinkOperation(long taskId, QueueSinkConfiguration configuration)
        {
            _taskId = taskId;
            _configuration = configuration;
        }

        public RavenCommand<UpdateQueueSinkOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new UpdateQueueSinkCommand(conventions, _taskId, _configuration);
        }

        private class UpdateQueueSinkCommand : RavenCommand<UpdateQueueSinkOperationResult>, IRaftCommand
        {
            private readonly long _taskId;
            private readonly QueueSinkConfiguration _configuration;
            private readonly DocumentConventions _conventions;

            public UpdateQueueSinkCommand(DocumentConventions conventions, long taskId, QueueSinkConfiguration configuration)
            {
                _taskId = taskId;
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/queue-sink?id={_taskId}";

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

                Result = JsonDeserializationClient.UpdateQueueSinkOperationResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }

    public class UpdateQueueSinkOperationResult
    {
        public long RaftCommandIndex { get; set; }

        public long TaskId { get; set; }
    }
}
