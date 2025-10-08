using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Exceptions;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.SchemaValidation;

internal abstract class AbstractSchemaValidationHandlerProcessorForValidate<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected ValidateSchemaValidationOperation.Parameters Parameters;

    protected AbstractSchemaValidationHandlerProcessorForValidate([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var operationId = RequestHandler.GetLongQueryString("operationId", required: false) ?? GetNextOperationId();

            var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "SchemaValidation/Validate");
            Parameters = JsonDeserializationServer.Parameters.ValidateSchemaValidationOperationParameters(json);

            ValidateParameters();

            var token = RequestHandler.CreateBackgroundOperationToken();

            StartValidationOperation(operationId, token);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, RequestHandler.ServerStore.NodeTag);
            }
        }
    }

    private void ValidateParameters()
    {
        if (string.IsNullOrWhiteSpace(Parameters.Collection) == false 
            && string.IsNullOrWhiteSpace(Parameters.SchemaDefinition) == false 
            && (Parameters.MaxErrorMessages.HasValue == false || Parameters.MaxErrorMessages.Value >= 0) 
            && (Parameters.MaxDurationInMinutes.HasValue == false || Parameters.MaxDurationInMinutes.Value > 0 )
            // Optional per-read-batch duration limit in seconds.
            && (Parameters.MaxReadBatchDurationInSeconds.HasValue == false || Parameters.MaxReadBatchDurationInSeconds.Value > 0))
            return;
        
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(Parameters.Collection))
            errors.Add($"Missing required parameter '{nameof(Parameters.Collection)}'.");
        if (string.IsNullOrWhiteSpace(Parameters.SchemaDefinition))
            errors.Add($"Missing required parameter '{nameof(Parameters.SchemaDefinition)}'.");

        if (Parameters.MaxErrorMessages is < 0)
            errors.Add($"Parameter '{nameof(Parameters.MaxErrorMessages)}' must be non-negative.");
        if (Parameters.MaxDurationInMinutes is <= 0)
            errors.Add($"Parameter '{nameof(Parameters.MaxDurationInMinutes)}' must be greater than 0.");
        if (Parameters.MaxReadBatchDurationInSeconds is <= 0)
            errors.Add($"Parameter '{nameof(Parameters.MaxReadBatchDurationInSeconds)}' must be greater than 0.");

        throw new BadRequestException("Invalid schema validation parameters:" + string.Join(", ", errors));
    }

    protected abstract void StartValidationOperation(long operationId, OperationCancelToken token);

    protected abstract long GetNextOperationId();
}
