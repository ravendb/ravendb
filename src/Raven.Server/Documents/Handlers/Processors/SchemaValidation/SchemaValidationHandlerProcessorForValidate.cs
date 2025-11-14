using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.SchemaValidation;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Sync;

namespace Raven.Server.Documents.Handlers.Processors.SchemaValidation;

internal sealed class SchemaValidationHandlerProcessorForValidate : AbstractSchemaValidationHandlerProcessorForValidate<DatabaseRequestHandler, DocumentsOperationContext>
{
    public SchemaValidationHandlerProcessorForValidate([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
    
    protected override void StartValidationOperation(long operationId, OperationCancelToken token)
    {
        _ = RequestHandler.Database.Operations.AddLocalOperation(
            operationId,
            OperationType.ValidateSchema,
            $"Schema validation for collection '{Parameters.Collection}' '{RequestHandler.Database.Name}'",
            detailedDescription: null,
            StartValidation,
            token: token).ContinueWith(_ => token.Dispose());
    }

    private Task<IOperationResult> StartValidation(Action<IOperationProgress> onProgress)
    {
        var maxErrorsMsg = Parameters.MaxErrorMessages ?? 1024;
        var maxToValidate = Parameters.MaxDocumentsToValidate ?? long.MaxValue;
        var etag = Parameters.StartEtag ?? 0L;
        
        var maxReadTrxTime = TimeSpan.FromSeconds(16);
        
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var errors = new Dictionary<string, string>();

            var stop = Stopwatch.StartNew();
            
            var progressReportRate = TimeSpan.FromSeconds(5);
            var nextProgressReport = TimeSpan.Zero;
            
            var actualErrorCount = 0L;
            var totalValidated = 0L;
            
            using var blittable = context.Sync.ReadForMemory(Parameters.SchemaDefinition, "schema-validation");
            var schemaValidator = SchemaValidationHelper.InitValidatorForDocument(context, blittable, Parameters.SchemaDefinition);

            using var errorBuilder = new ErrorBuilder(context);
            
            while (totalValidated < maxToValidate)
            {
                using (context.OpenReadTransaction())
                {
                    var stopTrxTime = stop.Elapsed + maxReadTrxTime;
                    var enumerable = context.DocumentDatabase.DocumentsStorage.GetDocumentsFrom(context, Parameters.Collection, etag, 0, long.MaxValue, DocumentFields.Id | DocumentFields.Data);
                    using var enumerator = enumerable.GetEnumerator();

                    bool hasMore;
                    while (true)
                    {
                        hasMore = enumerator.MoveNext();
                        if(hasMore == false)
                            break;
                        
                        if (stop.Elapsed > nextProgressReport)
                        {
                            onProgress(new ValidateSchemaProgress
                            {
                                ValidatedCount = totalValidated,
                                ErrorCount = actualErrorCount
                            });
                            nextProgressReport = stop.Elapsed + progressReportRate;
                        }
                        
                        using var document = enumerator.Current;
                        
                        etag = document.Etag;
                        if (actualErrorCount < maxErrorsMsg)
                        {
                            errorBuilder.Reset();
                            if (schemaValidator.Validate(document.Data, errorBuilder) == false)
                            {
                                errors[document.Id] = errorBuilder.ToString();
                                ++actualErrorCount;
                            }
                        }
                        else
                        {
                            if (schemaValidator.Validate(document.Data, null) == false)
                                ++actualErrorCount;
                        }

                        ++totalValidated;

                        if(totalValidated >= maxToValidate || stop.Elapsed > stopTrxTime)
                            break;
                    }
                    if(hasMore == false)
                        break;
                }
            }
            return Task.FromResult<IOperationResult>(new ValidateSchemaResult
            {
                Errors = errors,
                ErrorCount = actualErrorCount,
                ValidatedCount = totalValidated,
                LastEtag = etag
            });
        }
    }

    protected override long GetNextOperationId() => RequestHandler.Database.Operations.GetNextOperationId();
}
