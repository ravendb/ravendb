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
/// Adds an embeddings generation ETL task to a database.
/// </summary>
public class AddEmbeddingsGenerationOperation(EmbeddingsGenerationConfiguration configuration) : IMaintenanceOperation<AddEmbeddingsGenerationOperationResult>
{
    /// <summary>
    /// Creates the command to send to the server.
    /// </summary>
    public RavenCommand<AddEmbeddingsGenerationOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new AddEmbeddingsGenerationCommand(conventions, configuration);
    }

    internal class AddEmbeddingsGenerationCommand : RavenCommand<AddEmbeddingsGenerationOperationResult>, IRaftCommand
    {
        private readonly DocumentConventions _conventions;
        private readonly EmbeddingsGenerationConfiguration _configuration;

        public AddEmbeddingsGenerationCommand(DocumentConventions conventions, EmbeddingsGenerationConfiguration configuration)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/etl";

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

            Result = JsonDeserializationClient.AddEmbeddingsGenerationOperationResult(response);
        }

        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }
}
