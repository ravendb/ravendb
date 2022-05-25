using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Studio
{
    internal abstract class AbstractStudioCollectionFieldsHandlerProcessorForGetCollectionFields<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractStudioCollectionFieldsHandlerProcessorForGetCollectionFields([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected const int MaxArrayItemsToFetch = 16;

        protected abstract ValueTask<Dictionary<LazyStringValue, FieldType>> GetFieldsAsync(TOperationContext context, string collection, string prefix);
        
        public override async ValueTask ExecuteAsync()
        {
            var collection = RequestHandler.GetStringQueryString("collection", required: false);
            var prefix = RequestHandler.GetStringQueryString("prefix", required: false);

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var fields = await GetFieldsAsync(context, collection, prefix);

                if (fields == null)
                    return;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    var first = true;
                    foreach (var field in fields)
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        writer.WritePropertyName(field.Key);
                        writer.WriteString(field.Value.ToString());
                    }

                    writer.WriteEndObject();
                }
            }
        }
    }

    [Flags]
    public enum FieldType
    {
        None = 0,
        Object = 1 << 0,
        Array = 1 << 1,
        String = 1 << 2,
        Number = 1 << 3,
        Boolean = 1 << 4,
        Null = 1 << 5,
        ArrayObject = 1 << 6,
        ArrayArray = 1 << 7,
        ArrayString = 1 << 8,
        ArrayNumber = 1 << 9,
        ArrayBoolean = 1 << 10,
    }
}
