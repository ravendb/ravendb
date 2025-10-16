using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.SchemaValidation;

internal sealed class ShardedSchemaValidationHandlerProcessorForValidate : AbstractSchemaValidationHandlerProcessorForValidate<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedSchemaValidationHandlerProcessorForValidate([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override void StartValidationOperation(long operationId, OperationCancelToken token)
    {
        if (Parameters.StartEtag != null)
            throw new BadRequestException($"Parameter '{nameof(Parameters.StartEtag)}' is not supported for schema validation on a sharded database.");
        
        Parameters.MaxErrorMessages = Parameters.MaxErrorMessages.HasValue 
            ? Math.Max(1, Parameters.MaxErrorMessages.Value / RequestHandler.DatabaseContext.ShardCount) 
            : null;
        
        _ = RequestHandler.DatabaseContext.Operations.AddRemoteOperation<OperationIdResult<StartValidateSchemaOperationResult>, ValidateSchemaResult, ValidateSchemaValidationProgress>(
                operationId,
                OperationType.ValidateSchemaValidation,
                $"Schema validation for collection '{Parameters.Collection}' '{RequestHandler.DatabaseName}'",
                detailedDescription: null,
                commandFactory: (_, _) => new ValidateSchemaOperation.ValidateSchemaCommand(RequestHandler.ShardExecutor.Conventions, Parameters, operationId),
                token)
            .ContinueWith(_ => { token.Dispose(); });
    }

    protected override long GetNextOperationId() => RequestHandler.DatabaseContext.Operations.GetNextOperationId();
}
