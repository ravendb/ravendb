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

    public SchemaValidatorCache(DocumentsContextPool contextPool, RavenLogger logger)
    {
        _context.Return = contextPool.AllocateOperationContext(out _context.Value);
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
        
        foreach ((string collection, SchemaDefinition validator) in configuration.ValidatorsPerCollection)
        {
            if (_schemaValidatorsPerCollection.TryGetValue(collection, out var existingValidator)
                && validator.Schema.Equals(existingValidator.SchemaDefinition))
                continue;

            var schemaValidator = new SchemaValidator(validator.Disabled) { SchemaDefinition = validator.Schema };

            try
            {
                var blittable = _context.Value.Sync.ReadForMemory(validator.Schema, "schema-validation");
                EnsureMetadataIsValid(ref blittable);
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
            _schemaValidatorsPerCollection = newSchemaValidators.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
    }

    private void EnsureMetadataIsValid(ref BlittableJsonReaderObject blittable)
    {
        if (blittable.TryGet(SchemaValidatorConstants.AdditionalProperties, out object additionalProperties) == false 
            || additionalProperties is true)
            //If additional properties are allowed, no problematic restriction on metadata is can be.
            return;

        if (blittable.TryGet(SchemaValidatorConstants.Properties, out BlittableJsonReaderObject properties)
            && properties.Contains(Client.Constants.Documents.Metadata.Key))
            //If explicit restriction on metadata is configured we don't verify it in this point.
            return;
        
        
        object newProperties;
        if (properties == null)
        {
            newProperties = new DynamicJsonValue { [Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue() };
        }
        else
        {
            properties.Modifications = new DynamicJsonValue(properties) { [Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue() };
            newProperties = properties;
        }
        
        blittable.Modifications = new DynamicJsonValue(blittable) { [SchemaValidatorConstants.Properties] = newProperties };

        using (_ = blittable)
            blittable = _context.Value.ReadObject(blittable, "modified-schema-validation");
    }

    public void Validate(string collection, BlittableJsonReaderObject document, NonPersistentDocumentFlags nonPersistentFlags, DocumentsOperationContext context)
    {
        // TODO: check if we need to add more flags here
        if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication) ||
            nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromResharding))
            return;

        if (_schemaValidatorsPerCollection == null || _disabled)
            return;

        if (_schemaValidatorsPerCollection.TryGetValue(collection, out var validator) == false)
            return;

        if (validator.Disabled)
            return;

        using (var errorBuilder = new ErrorBuilder(context))
        {
            if (validator.Validate(document, errorBuilder))
                return;

            throw new SchemaValidationException(errorBuilder.GetErrors().ToString());
        }
    }

    public void Dispose()
    {
        using (_context.Return)
        {
        }
    }
}
