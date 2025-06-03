using System;
using System.Collections.Generic;
using Raven.Server.Documents;
using Raven.Server.SchemaValidation.ErrorMessage;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.SchemaValidation;

public class SchemaValidatorCache : IDisposable
{
    private readonly (IDisposable Return, JsonOperationContext Value) _context;
    private Dictionary<string, SchemaValidator> _schemaValidatorsPerCollection;
    private bool _disabled;

    public SchemaValidatorCache(DocumentsContextPool contextPool)
    {
        _context.Return = contextPool.AllocateOperationContext(out _context.Value);
    }

    public void Update(Client.Documents.Operations.SchemaValidation.SchemaValidationConfiguration configuration)
    {
        if (configuration == null)
            return;

        _disabled = configuration.Disabled;

        if (configuration.ValidatorsByCollection == null || configuration.ValidatorsByCollection.Count == 0)
        {
            _schemaValidatorsPerCollection?.Clear();
            return;
        }

        List<(string Collection, SchemaValidator SchemaValidator)> schemaValidatorsToAdd = null;
        _schemaValidatorsPerCollection ??= new Dictionary<string, SchemaValidator>();

        foreach ((string collection, Client.Documents.Operations.SchemaValidation.SchemaValidationConfiguration.Validator validator) in configuration.ValidatorsByCollection)
        {
            if (_schemaValidatorsPerCollection.TryGetValue(collection, out var existingValidator))
            {
                if (validator.SchemaDefinition.Equals(existingValidator.SchemaDefinition))
                    continue;
            }

            var schemaValidator = new SchemaValidator(validator.Disabled)
            {
                SchemaDefinition = validator.SchemaDefinition
            };

            //TODO: don't throw if it fails here
            var blittable = _context.Value.Sync.ReadForMemory(validator.SchemaDefinition, "my-raw-json");
            schemaValidator.Init(blittable);

            schemaValidatorsToAdd ??= new List<(string, SchemaValidator)>();
            schemaValidatorsToAdd.Add((collection, schemaValidator));
        }

        Dictionary<string, SchemaValidator> newSchemaValidators = null;

        foreach (var existing in _schemaValidatorsPerCollection)
        {
            if (configuration.ValidatorsByCollection.ContainsKey(existing.Key) == false)
            {
                newSchemaValidators ??= new Dictionary<string, SchemaValidator>(_schemaValidatorsPerCollection);
                newSchemaValidators.Remove(existing.Key);
            }
        }

        if (schemaValidatorsToAdd != null)
        {
            newSchemaValidators ??= new Dictionary<string, SchemaValidator>(_schemaValidatorsPerCollection);

            foreach (var keyValue in schemaValidatorsToAdd)
            {
                newSchemaValidators[keyValue.Collection] = keyValue.SchemaValidator;
            }

        }

        if (newSchemaValidators != null)
            _schemaValidatorsPerCollection = newSchemaValidators;
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

            //TODO: a dedicated exception
            throw new InvalidOperationException(errorBuilder.GetErrors().ToString());
        }
    }

    public void Dispose()
    {
        using (_context.Return)
        {
        }
    }
}
