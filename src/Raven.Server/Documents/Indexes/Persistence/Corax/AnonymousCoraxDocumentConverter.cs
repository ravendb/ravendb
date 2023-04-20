using System;
using Amazon.SimpleNotificationService.Model;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Utils;
using Constants = Raven.Client.Constants;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public sealed class AnonymousCoraxDocumentConverter : AnonymousCoraxDocumentConverterBase
{
    public AnonymousCoraxDocumentConverter(Index index, bool storeValue = false) : base(index, numberOfBaseFields: 1, storeValue: storeValue)
    {
    }
}

public abstract class AnonymousCoraxDocumentConverterBase : CoraxDocumentConverterBase
{
    private readonly bool _isMultiMap;
    private IPropertyAccessor _propertyAccessor;

    public AnonymousCoraxDocumentConverterBase(Index index, int numberOfBaseFields = 1, string keyFieldName = null, bool storeValue = false, string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName, bool canContainSourceDocumentId = false) : base(index, storeValue, index.Configuration.IndexEmptyEntries, true, 1, keyFieldName, Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName, canContainSourceDocumentId)
    {
        _isMultiMap = index.IsMultiMap;
    }

    public override ByteStringContext<ByteStringMemoryCache>.InternalScope SetDocumentFields(
        LazyStringValue key, LazyStringValue sourceDocumentId,
        object doc, JsonOperationContext indexContext, out LazyStringValue id,
        out ByteString output, out float? documentBoost)
    {
        var boostedValue = doc as BoostedValue;
        var documentToProcess = boostedValue == null ? doc : boostedValue.Value;
        id = default;
        documentBoost = null;
        
        IPropertyAccessor accessor;
        if (_isMultiMap == false)
            accessor = _propertyAccessor ??= PropertyAccessor.Create(documentToProcess.GetType(), documentToProcess);
        else
            accessor = TypeConverter.GetPropertyAccessor(documentToProcess);

        var scope = new SingleEntryWriterScope(Allocator);
        var storedValue = _storeValue ? new DynamicJsonValue() : null;

        var knownFields = GetKnownFieldsForWriter();

        // We prepare for the next entry.
        ref var entryWriter = ref GetEntriesWriter();

        try
        {
            if (boostedValue != null)
                documentBoost = boostedValue.Boost;

            foreach (var property in accessor.GetProperties(documentToProcess))
            {
                var value = property.Value;

                if (_fields.TryGetValue(property.Key, out var field) == false)
                    throw new InvalidOperationException($"Field '{property.Key}' is not defined. Available fields: {string.Join(", ", _fields.Keys)}.");

                
                InsertRegularField(field, value, indexContext, ref entryWriter, scope, out var shouldSkip);
                if (storedValue is not null && shouldSkip == false)
                {
                    //Notice: we are always saving values inside Corax index. This method is explicitly for MapReduce because we have to have JSON as the last item.
                    var blittableValue = TypeConverter.ToBlittableSupportedType(value, out TypeConverter.BlittableSupportedReturnType returnType, flattenArrays: true);

                    if (returnType != TypeConverter.BlittableSupportedReturnType.Ignored)
                        storedValue[property.Key] = blittableValue;
                }
            }

            if (storedValue is not null)
            {
                var bjo = indexContext.ReadObject(storedValue, "corax field as json");
                scope.Write(string.Empty, knownFields.Count - 1, bjo, ref entryWriter);
            }

            if (entryWriter.IsEmpty() == true)
            {
                output = default;
                return default;
            }

            id = key ?? throw new InvalidParameterException("Cannot find any identifier of the document.");
            if (sourceDocumentId != null && knownFields.TryGetByFieldName(Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName, out var documentSourceField))
                scope.Write(string.Empty, documentSourceField.FieldId, sourceDocumentId.AsSpan(), ref entryWriter);
            
            scope.Write(string.Empty, 0, id.AsSpan(), ref entryWriter);
            return entryWriter.Finish(out output);
        }
        catch
        {
            ResetEntriesWriter();
            throw;
        }
    }
}
