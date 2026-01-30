using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.SchemaValidation;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Studio.Processors;

internal abstract class AbstractStudioDocumentHandlerProcessorForValidateDocument<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    protected AbstractStudioDocumentHandlerProcessorForValidateDocument([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract SchemaValidatorCache SchemaValidatorCache { get; }

    public override async ValueTask ExecuteAsync()
    {
        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var document = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "document-to-validate");
            var collectionName = CollectionName.GetCollectionName(document);

            var result = new ValidateDocumentResult();
            var cache = SchemaValidatorCache;
            if (cache == null || cache.IsSchemaEnabledForAny([collectionName]) == false)
            {
                result.Status = ValidateDocumentResult.ValidationStatus.MissingSchema;
            }
            else
            {
                var errorBuilder = new ErrorBuilder();
                if (cache.Validate(collectionName, document, errorBuilder))
                {
                    result.Status = ValidateDocumentResult.ValidationStatus.Valid;
                }
                else
                {
                    result.Status = ValidateDocumentResult.ValidationStatus.Invalid;
                    result.ErrorMessages = errorBuilder.GetErrors().ToList();
                }
            }

            await using (AsyncBlittableJsonTextWriter writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }
    }
}

public class ValidateDocumentResult : IDynamicJson
{
    public ValidationStatus Status { get; set; }

    public List<string> ErrorMessages { get; set; }

    public enum ValidationStatus
    {
        Valid,
        MissingSchema,
        Invalid
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Status)] = Status.ToString(),
            [nameof(ErrorMessages)] = ErrorMessages
        };
    }
}
