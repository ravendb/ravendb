using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.SchemaValidation;
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
        Parameters.MaxErrorMessages = Parameters.MaxErrorMessages.HasValue 
            ? Math.Max(1, Parameters.MaxErrorMessages.Value / RequestHandler.DatabaseContext.ShardCount) 
            : null;
        
        _ = RequestHandler.DatabaseContext.Operations.AddRemoteOperation<OperationIdResult<StartValidateSchemaValidationOperationResult>, ValidateSchemaValidationResult, ValidateSchemaValidationProgress>(
                operationId,
                OperationType.ValidateSchemaValidation,
                $"Schema validation for collection '{Parameters.Collection}' '{RequestHandler.DatabaseName}'",
                detailedDescription: null,
                commandFactory: (_, _) => new ValidateSchemaValidationOperation.ValidateSchemaValidationCommand(RequestHandler.ShardExecutor.Conventions, Parameters, operationId),
                token)
            .ContinueWith(_ => { token.Dispose(); });
    }

    protected override long GetNextOperationId() => RequestHandler.DatabaseContext.Operations.GetNextOperationId();
}
