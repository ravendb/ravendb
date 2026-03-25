using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Adds a GenAI task to a database.
/// </summary>
public class AddGenAiOperation(GenAiConfiguration configuration, StartingPointChangeVector startingPoint = null) : IMaintenanceOperation<AddGenAiOperationResult>
{
    /// <summary>
    /// Creates the command to send to the server.
    /// </summary>
    public RavenCommand<AddGenAiOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new AddGenAiCommand(conventions, configuration, startingPoint);
    }

    internal sealed class AddGenAiCommand : RavenCommand<AddGenAiOperationResult>, IRaftCommand
    {
        private readonly DocumentConventions _conventions;
        private readonly StartingPointChangeVector _startingPoint;
        private readonly GenAiConfiguration _configuration;

        public AddGenAiCommand(DocumentConventions conventions, GenAiConfiguration configuration, StartingPointChangeVector startingPoint = null)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _startingPoint = startingPoint ?? StartingPointChangeVector.LastDocument;
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/etl?changeVector={Uri.EscapeDataString(_startingPoint.Value)}";
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

            Result = JsonDeserializationClient.AddGenAiOperationResult(response);
        }

        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }
}
