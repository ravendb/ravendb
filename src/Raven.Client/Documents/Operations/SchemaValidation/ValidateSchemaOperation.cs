using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.SchemaValidation;

/// <summary>
/// Starts a background operation that validates documents in the specified collection against the provided JSON schema.
/// Optional limits (if omitted server defaults are used): MaxErrorMessages=1024, MaxDocumentsToValidate=unlimited.
/// You may also provide a starting Etag to continue validation from a point in the collection; by default validation starts from the first document.
/// </summary>
public sealed class ValidateSchemaOperation : IMaintenanceOperation<OperationIdResult<StartValidateSchemaOperationResult>>
{
    private readonly Parameters _parameters;

    /// <summary>
    /// Parameters for schema validation. Schema and Collection are required; other values are optional.
    /// </summary>
    public sealed class Parameters
    {
        /// <summary>JSON schema definition. (Required)</summary>
        public string SchemaDefinition { get; set; }

        /// <summary>Target collection to validate. (Required)</summary>
        public string Collection { get; set; }
        
        /// <summary>Maximum collected validation error messages. (Optional, default 1024, must be >= 0 when specified)</summary>
        public int? MaxErrorMessages { get; set; }

        /// <summary>Maximum number of documents to validate (Optional, default: unlimited, must be > 0 when specified).</summary>
        public long? MaxDocumentsToValidate { get; set; }
        
        /// <summary>Starting document etag to begin validation from. (Optional, default: start from the first document)</summary>
        public long? StartEtag { get; set; }
    }

    /// <summary>
    /// Create the operation.
    /// </summary>
    public ValidateSchemaOperation(Parameters parameters)
    {
        _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));

        if (string.IsNullOrWhiteSpace(_parameters.SchemaDefinition))
            throw new ArgumentException("Schema must be provided.", nameof(parameters));
        if (string.IsNullOrWhiteSpace(_parameters.Collection))
            throw new ArgumentException("Collection must be provided.", nameof(parameters));

        if (_parameters.MaxErrorMessages is < 0)
            throw new ArgumentOutOfRangeException(nameof(parameters), $"Property {nameof(parameters.MaxErrorMessages)} must be >= 0.");
        if (_parameters.MaxDocumentsToValidate is <= 0)
            throw new ArgumentOutOfRangeException(nameof(parameters), $"Property {nameof(parameters.MaxDocumentsToValidate)} must be > 0.");
    }

    public RavenCommand<OperationIdResult<StartValidateSchemaOperationResult>> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
    {
        return new ValidateSchemaCommand(conventions, _parameters);
    }

    internal class ValidateSchemaCommand : RavenCommand<OperationIdResult<StartValidateSchemaOperationResult>>, IRaftCommand
    {
        private readonly DocumentConventions _conventions;
        private readonly Parameters _parameters;
        private readonly long? _operationId;

        public ValidateSchemaCommand(DocumentConventions conventions, Parameters parameters, long? operationId = null)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
            _operationId = operationId;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/schema-validation/validate";

            if (_operationId.HasValue)
                url += $"?operationId={_operationId}";
            
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_parameters, ctx)).ConfigureAwait(false), _conventions)
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            var result = JsonDeserializationClient.StartValidateSchemaOperationResult(response);
            var operationIdResult = JsonDeserializationClient.OperationIdResult(response);

            Result = operationIdResult.ForResult(result);
        }

        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }
}

public sealed class StartValidateSchemaOperationResult
{
    public string ResponsibleNode { get; set; }

    public long OperationId { get; set; }
}
