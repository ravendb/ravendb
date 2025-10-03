using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Exceptions.SchemaValidation;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.SchemaValidation;

public class SchemaValidatorCache : IDisposable
{
    private static readonly FrozenDictionary<string, SchemaValidator> EmptyCache = Array.Empty<KeyValuePair<string, SchemaValidator>>().ToFrozenDictionary();
    
    private readonly RavenLogger _logger;
    private readonly (IDisposable Return, JsonOperationContext Value) _context;
    private FrozenDictionary<string, SchemaValidator> _schemaValidatorsPerCollection = EmptyCache;
    private bool _disabled;

    public static SchemaValidatorCache Create<T>(JsonContextPoolBase<T> contextPool, RavenLogger logger)
        where T : JsonOperationContext
    {
        var returnContext = contextPool.AllocateOperationContext(out JsonOperationContext context);
        return new SchemaValidatorCache(returnContext, context, logger);
    }
    
    private SchemaValidatorCache(IDisposable returnCtx, JsonOperationContext ctx, RavenLogger logger)
    {
        _context.Return = returnCtx;
        _context.Value = ctx;
        _logger = logger;
    }

    public void Update(SchemaValidationConfiguration configuration)
    {
        if (configuration == null)
            return;

        _disabled = configuration.Disabled;

        if (configuration.ValidatorsPerCollection == null || configuration.ValidatorsPerCollection.Count == 0)
        {
            _schemaValidatorsPerCollection = EmptyCache;
            return;
        }

        Dictionary<string, SchemaValidator> newSchemaValidators = null;
        
        foreach ((string collection, Client.Documents.Operations.SchemaValidation.SchemaDefinition validator) in configuration.ValidatorsPerCollection)
        {
            if (_schemaValidatorsPerCollection.TryGetValue(collection, out var existingValidator)
                && validator.Schema.Equals(existingValidator.SchemaDefinition))
                continue;

            var schemaValidator = new SchemaValidator(validator.Disabled) { SchemaDefinition = validator.Schema };

            try
            {
                var blittable = _context.Value.Sync.ReadForMemory(validator.Schema, "schema-validation");
                SchemaValidationHelper.EnsureMetadataIsValid(_context.Value, ref blittable);
                schemaValidator.Init(blittable);
            }
            catch (Exception e)
            {
                if (_logger.IsErrorEnabled)
                    _logger.Error($"Failed to parse the schema validator for collection {collection}", e);

                continue;
            }

            newSchemaValidators ??= new Dictionary<string, SchemaValidator>(_schemaValidatorsPerCollection);
            newSchemaValidators[collection] = schemaValidator;
        }

        foreach (var existing in _schemaValidatorsPerCollection)
        {
            if (configuration.ValidatorsPerCollection.ContainsKey(existing.Key) == false)
            {
                newSchemaValidators ??= new Dictionary<string, SchemaValidator>(_schemaValidatorsPerCollection);
                newSchemaValidators.Remove(existing.Key);
            }
        }

        if (newSchemaValidators != null)
            _schemaValidatorsPerCollection = newSchemaValidators.ToFrozenDictionary();
    }

    public void Validate(string collection, BlittableJsonReaderObject document, NonPersistentDocumentFlags nonPersistentFlags, JsonOperationContext context)
    {
        // TODO: check if we need to add more flags here
        if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication) ||
            nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromResharding))
            return;

        if (Validate(context, collection, document, out var error) == false)
            throw new SchemaValidationException(error);
    }

    public bool Validate(JsonOperationContext context, string collection, BlittableJsonReaderObject document, out string error)
    {
        error = null;
        if (_schemaValidatorsPerCollection == null || _disabled)
            return true;

        if (_schemaValidatorsPerCollection.TryGetValue(collection, out var validator) == false)
            return true;

        if (validator.Disabled)
            return true;

        using (var errorBuilder = new ErrorBuilder(context))
        {
            if (validator.Validate(document, errorBuilder))
                return true;

            error = errorBuilder.GetErrors().ToString();
            return false;
        }
    }

    public bool Validate(JsonOperationContext context, string collection, BlittableJsonReaderObject document, ErrorBuilder errorBuilder)
    {
        if (_schemaValidatorsPerCollection == null || _disabled)
            return true;

        if (_schemaValidatorsPerCollection.TryGetValue(collection, out var validator) == false)
            return true;

        if (validator.Disabled)
            return true;

        return validator.Validate(document, errorBuilder);
    }

    public void Dispose()
    {
        using (_context.Return)
        {
        }
    }
}
