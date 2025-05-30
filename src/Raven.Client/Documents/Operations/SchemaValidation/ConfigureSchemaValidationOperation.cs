using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.SchemaValidation;

public class ConfigureSchemaValidationOperation : IMaintenanceOperation<ConfigureSchemaValidationOperationResult>
{
    private readonly SchemaValidationConfiguration _configuration;

    /// <inheritdoc cref="ConfigureSchemaValidationOperation"/>
    /// <param name="configuration">The schema validation configuration to apply.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="configuration"/> is null.</exception>
    public ConfigureSchemaValidationOperation(SchemaValidationConfiguration configuration)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    public RavenCommand<ConfigureSchemaValidationOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
    {
        return new ConfigureSchemaValidationCommand(conventions, _configuration);
    }

    private class ConfigureSchemaValidationCommand : RavenCommand<ConfigureSchemaValidationOperationResult>, IRaftCommand
    {
        private readonly DocumentConventions _conventions;
        private readonly SchemaValidationConfiguration _configuration;

        public ConfigureSchemaValidationCommand(DocumentConventions conventions, SchemaValidationConfiguration configuration)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/schema-validation/config";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx)).ConfigureAwait(false), _conventions)
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.ConfigureSchemaValidationOperationResult(response);
        }

        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }
}
