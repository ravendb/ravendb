using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Exceptions.SchemaValidation;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.SchemaValidation;

public class SchemaValidatorCache : IDisposable
{
    private static readonly FrozenDictionary<string, SchemaValidator> EmptyCache = Array.Empty<KeyValuePair<string, SchemaValidator>>().ToFrozenDictionary();

    private readonly DatabaseNotificationCenter _notificationCenter;
    private readonly RavenLogger _logger;
    private readonly (IDisposable Return, JsonOperationContext Value) _context;
    private FrozenDictionary<string, SchemaValidator> _schemaValidatorsPerCollection = EmptyCache;
    public bool Disabled { get; private set; } = true;

    public static SchemaValidatorCache Create<T>(JsonContextPoolBase<T> contextPool, DatabaseNotificationCenter notificationCenter, RavenLogger logger)
        where T : JsonOperationContext
    {
        var returnContext = contextPool.AllocateOperationContext(out JsonOperationContext context);
        return new SchemaValidatorCache(returnContext, context, notificationCenter, logger);
    }
    
    private SchemaValidatorCache(IDisposable returnCtx, JsonOperationContext ctx, DatabaseNotificationCenter notificationCenter, RavenLogger logger)
    {
        _context.Return = returnCtx;
        _context.Value = ctx;
        _notificationCenter = notificationCenter;
        _logger = logger;
    }

    public void Update(SchemaValidationConfiguration configuration)
    {
        if (configuration == null)
            return;

        Disabled = configuration.Disabled;

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
            {
                existingValidator.Disabled = validator.Disabled;
                continue;
            }

            SchemaValidator schemaValidator;
            try
            {
                var blittable = _context.Value.Sync.ReadForMemory(validator.Schema, "schema-validation");
                schemaValidator = SchemaValidationHelper.InitValidatorForDocument(_context.Value, blittable, validator.Schema, validator.Disabled);
            }
            catch (Exception e)
            {
                var errorMessage = $"Failed to parse the schema validator for collection {collection}";
                
                if (_logger.IsErrorEnabled)
                    _logger.Error(errorMessage, e);

                const string title = "Schema Validation Configuration";
                var alert = AlertRaised.Create(_notificationCenter.Database, title, errorMessage, AlertReason.SchemaValidationConfiguration_Error, NotificationSeverity.Error, details:new ExceptionDetails(e));
                _notificationCenter.Add(alert);
                
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

    public void Validate(string collection, BlittableJsonReaderObject document, NonPersistentDocumentFlags nonPersistentFlags, JsonOperationContext context)
    {
        if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.SkipSchemaValidation))
            return;

        if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromResharding))
            return;

        if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication) &&
            nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromExternalReplication) == false)
            return;

        if (Validate(context, collection, document, out var error) == false)
            throw new SchemaValidationException(error);
    }

    private bool Validate(JsonOperationContext context, string collection, BlittableJsonReaderObject document, out string error)
    {
        error = null;
        if (_schemaValidatorsPerCollection == null || Disabled)
            return true;

        if (_schemaValidatorsPerCollection.TryGetValue(collection, out var validator) == false)
            return true;

        if (validator.Disabled)
            return true;

        using (var errorBuilder = new ErrorBuilder(context))
        {
            if (validator.Validate(document, errorBuilder))
                return true;

            error = errorBuilder.GetError().ToString();
            return false;
        }
    }

    public bool Validate(string collection, BlittableJsonReaderObject document, ErrorBuilder errorBuilder)
    {
        if (_schemaValidatorsPerCollection == null || Disabled)
            return true;

        if (_schemaValidatorsPerCollection.TryGetValue(collection, out var validator) == false)
            return true;

        if (validator.Disabled)
            return true;

        return validator.Validate(document, errorBuilder);
    }
    
    public bool IsSchemaEnabledForAny(string[] collections)
    {
        if (Disabled)
            return false;

        var schemaValidatorsPerCollection = _schemaValidatorsPerCollection;
        if (collections == null)
            return schemaValidatorsPerCollection.Count(x => x.Value.Disabled == false) > 0;

        foreach (var collection in collections)
        {
            if (schemaValidatorsPerCollection.TryGetValue(collection, out var validator) && validator.Disabled == false)
                return true;
        }

        return false;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        using (_context.Return)
        {
        }
    }

    ~SchemaValidatorCache()
    {
        Dispose();
    }
}
