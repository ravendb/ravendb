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
            && string.IsNullOrWhiteSpace(Parameters.Schema) == false 
            && (Parameters.MaxErrorsMsg.HasValue == false || Parameters.MaxErrorsMsg.Value >= 0) 
            && (Parameters.MaxTimeInMinutes.HasValue == false || Parameters.MaxTimeInMinutes.Value > 0 )
            && (Parameters.MaxReadTrxTimeInSeconds.HasValue == false || Parameters.MaxReadTrxTimeInSeconds.Value > 0)) 
            return;
        
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(Parameters.Collection))
            errors.Add($"Missing required parameter '{nameof(Parameters.Collection)}'.");
        if (string.IsNullOrWhiteSpace(Parameters.Schema))
            errors.Add($"Missing required parameter '{nameof(Parameters.Schema)}'.");

        if (Parameters.MaxErrorsMsg is < 0)
            errors.Add($"Parameter '{nameof(Parameters.MaxErrorsMsg)}' must be non-negative.");
        if (Parameters.MaxTimeInMinutes is <= 0)
            errors.Add($"Parameter '{nameof(Parameters.MaxTimeInMinutes)}' must be greater than 0.");
        if (Parameters.MaxReadTrxTimeInSeconds is <= 0)
            errors.Add($"Parameter '{nameof(Parameters.MaxReadTrxTimeInSeconds)}' must be greater than 0.");

        throw new BadRequestException("Invalid schema validation parameters:" + string.Join(", ", errors));
    }

    protected abstract void StartValidationOperation(long operationId, OperationCancelToken token);

    protected abstract long GetNextOperationId();
}
