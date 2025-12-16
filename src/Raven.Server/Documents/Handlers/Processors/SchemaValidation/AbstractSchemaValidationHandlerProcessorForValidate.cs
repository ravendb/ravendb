using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Exceptions;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.SchemaValidation;

internal abstract class AbstractSchemaValidationHandlerProcessorForValidate<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected StartSchemaValidationOperation.Parameters Parameters;

    protected AbstractSchemaValidationHandlerProcessorForValidate([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var operationId = RequestHandler.GetLongQueryString("operationId", required: false) ?? GetNextOperationId();

            var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "SchemaValidation/Validate");
            Parameters = JsonDeserializationServer.Parameters.ValidateSchemaOperationParameters(json);
            ValidateParameters();

            if(RavenLogManager.Instance.IsAuditEnabled)
                RequestHandler.LogAuditForDatabase("VALIDATE", $"Start schema validation operation for collection '{Parameters.Collection}'");
            
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
            && (Parameters.MaxDocumentsToValidate.HasValue == false || Parameters.MaxDocumentsToValidate.Value > 0 ))
            return;
        
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(Parameters.Collection))
            errors.Add($"Missing required parameter '{nameof(Parameters.Collection)}'.");
        if (string.IsNullOrWhiteSpace(Parameters.SchemaDefinition))
            errors.Add($"Missing required parameter '{nameof(Parameters.SchemaDefinition)}'.");

        if (Parameters.MaxErrorMessages is < 0)
            errors.Add($"Parameter '{nameof(Parameters.MaxErrorMessages)}' must be non-negative.");
        if (Parameters.MaxDocumentsToValidate is <= 0)
            errors.Add($"Parameter '{nameof(Parameters.MaxDocumentsToValidate)}' must be greater than 0.");

        throw new BadRequestException("Invalid schema validation parameters:\n" + string.Join("\n", errors));;
    }

    protected abstract void StartValidationOperation(long operationId, OperationCancelToken token);

    protected abstract long GetNextOperationId();
}
