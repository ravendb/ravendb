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
/// Starts a server-side background operation that validates documents of the specified collection
/// against the provided JSON schema. Returns an operation id for status tracking.
/// When optional limits are not supplied the server applies defaults:
/// MaxErrorsMsg = 1024, MaxTimeInMinutes = 16, MaxReadTrxTimeInSeconds = 960.
/// </summary>
public sealed class ValidateSchemaValidationOperation : IMaintenanceOperation<OperationIdResult<StartValidateSchemaValidationOperationResult>>
{
    private readonly Parameters _parameters;

    /// <summary>
    /// Parameters for schema validation. Schema and Collection are required; other values are optional.
    /// </summary>
    public sealed class Parameters
    {
        /// <summary>JSON schema definition text. (Required)</summary>
        public string Schema { get; set; }

        /// <summary> Maximum collected validation error messages. (Optional, default 1024, must be > 0 when specified) </summary>
        public int? MaxErrorsMsg { get; set; }

        /// <summary>Target collection to validate. (Required)</summary>
        public string Collection { get; set; }

        /// <summary>Total time limit in minutes. (Optional, default 16, must be > 0 when specified)</summary>
        public int? MaxTimeInMinutes { get; set; }

        /// <summary>Per read transaction time limit in seconds. (Optional, default 960, must be > 0 when specified)</summary>
        public int? MaxReadTrxTimeInSeconds { get; set; }
    }

    /// <summary>
    /// Create the operation.
    /// </summary>
    public ValidateSchemaValidationOperation(Parameters parameters)
    {
        _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));

        if (string.IsNullOrWhiteSpace(_parameters.Schema))
            throw new ArgumentException("Schema must be provided.", nameof(parameters));
        if (string.IsNullOrWhiteSpace(_parameters.Collection))
            throw new ArgumentException("Collection must be provided.", nameof(parameters));

        if (_parameters.MaxErrorsMsg is < 0)
            throw new ArgumentOutOfRangeException(nameof(parameters), $"Property {nameof(parameters.MaxErrorsMsg)} must be >= 0.");
        if (_parameters.MaxTimeInMinutes is <= 0)
            throw new ArgumentOutOfRangeException(nameof(parameters), $"Property {nameof(parameters.MaxTimeInMinutes)} must be > 0.");
        if (_parameters.MaxReadTrxTimeInSeconds is <= 0)
            throw new ArgumentOutOfRangeException(nameof(parameters), $"Property {nameof(parameters.MaxReadTrxTimeInSeconds)} must be > 0.");
    }

    public RavenCommand<OperationIdResult<StartValidateSchemaValidationOperationResult>> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
    {
        return new ValidateSchemaValidationCommand(conventions, _parameters);
    }

    internal class ValidateSchemaValidationCommand : RavenCommand<OperationIdResult<StartValidateSchemaValidationOperationResult>>, IRaftCommand
    {
        private readonly DocumentConventions _conventions;
        private readonly Parameters _parameters;
        private readonly long? _operationId;

        public ValidateSchemaValidationCommand(DocumentConventions conventions, Parameters parameters, long? operationId = null)
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

            var result = JsonDeserializationClient.StartValidateSchemaValidationOperationResult(response);
            var operationIdResult = JsonDeserializationClient.OperationIdResult(response);

            // OperationNodeTag used to fetch operation status
            operationIdResult.OperationNodeTag ??= result.ResponsibleNode;
            Result = operationIdResult.ForResult(result);
        }

        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }
}

public sealed class StartValidateSchemaValidationOperationResult
{
    public string ResponsibleNode { get; set; }

    public long OperationId { get; set; }
}
