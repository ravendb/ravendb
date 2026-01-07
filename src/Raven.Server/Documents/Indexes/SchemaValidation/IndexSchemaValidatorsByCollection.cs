using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.SchemaValidation;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Sync;

namespace Raven.Server.Documents.Indexes.SchemaValidation;

public sealed class IndexSchemaValidatorsByCollection : IDisposable
{
    private readonly Dictionary<string, SchemaValidator> _validators;

    private readonly IDisposable _returnContext; // holds allocated TransactionOperationContext lifetime

    private IndexSchemaValidatorsByCollection(IDisposable returnContext, Dictionary<string, SchemaValidator> validators)
    {
        _returnContext = returnContext;
        _validators = validators;
    }

    public static IndexSchemaValidatorsByCollection Create(TransactionContextPool ctxPool, IndexSchemaDefinitions definitions, SchemaValidationConfiguration config)
    {
        var returnContext = ctxPool.AllocateOperationContext(out TransactionOperationContext context);
        var validators = new Dictionary<string, SchemaValidator>(StringComparer.OrdinalIgnoreCase);
        try
        {
            foreach (var (collection, definition) in definitions)
            {
                var blittable = context.Sync.ReadForMemory(definition, "schema-validation");
                var validator = SchemaValidationHelper.InitValidatorForDocument(context, blittable, definition, config);
                validators[collection] = validator;
            }

            return new IndexSchemaValidatorsByCollection(returnContext, validators);
        }
        catch
        {
            returnContext.Dispose();
            throw;
        }
    }

    public bool TryGet(string collection, out SchemaValidator validator) => _validators.TryGetValue(collection, out validator);

    public void Dispose()
    {
        _returnContext?.Dispose();
        _validators.Clear();
    }
}

