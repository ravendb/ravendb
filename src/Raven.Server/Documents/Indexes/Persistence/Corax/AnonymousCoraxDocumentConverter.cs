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

    public AnonymousCoraxDocumentConverterBase(Index index, int numberOfBaseFields = 1, string keyFieldName = null, bool storeValue = false, string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName, bool canContainSourceDocumentId = false) : base(index, storeValue, indexImplicitNull: index.Configuration.IndexMissingFieldsAsNull, index.Configuration.IndexEmptyEntries, 1, keyFieldName, Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName, canContainSourceDocumentId)
    {
        _isMultiMap = index.IsMultiMap;
    }

    public override ByteStringContext<ByteStringMemoryCache>.InternalScope SetDocumentFields(
        LazyStringValue key, LazyStringValue sourceDocumentId,
        object doc, JsonOperationContext indexContext, object sourceDocument, out LazyStringValue id,
        out ByteString output, out float? documentBoost, out int fields)
    {
        var boostedValue = doc as BoostedValue;
        var documentToProcess = boostedValue == null ? doc : boostedValue.Value;
        id = default;
        documentBoost = null;
        
        // It is important to note that as soon as an accessor is created this instance is tied to the underlying property type.
        // This optimization is not able to handle differences in types for the same property. Therefore, this instances cannot
        // be reused for Map and Reduce documents at the same time. You need a new instance to do so. 
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

            bool shouldSkip = _indexEmptyEntries == false;
            foreach (var property in accessor.GetProperties(documentToProcess))
            {
                var value = property.Value;

                if (_fields.TryGetValue(property.Key, out var field) == false)
                    throw new InvalidOperationException($"Field '{property.Key}' is not defined. Available fields: {string.Join(", ", _fields.Keys)}.");

                
                InsertRegularField(field, value, indexContext, ref entryWriter, sourceDocument, scope, out var innerShouldSkip);
                shouldSkip &= innerShouldSkip;
                
                
                if (storedValue is not null && innerShouldSkip == false)
                {
                    //Notice: we are always saving values inside Corax index. This method is explicitly for MapReduce because we have to have JSON as the last item.
                    var blittableValue = TypeConverter.ToBlittableSupportedType(value, out TypeConverter.BlittableSupportedReturnType returnType, flattenArrays: true);

                    if (returnType != TypeConverter.BlittableSupportedReturnType.Ignored)
                        storedValue[property.Key] = blittableValue;
                }
            }

            if (shouldSkip && entryWriter.IsEmpty() && _indexEmptyEntries == false)
            {
                fields = 0;
                output = default;
                return default;
            }
            
            if (storedValue is not null)
            {
                var bjo = indexContext.ReadObject(storedValue, "corax field as json");
                scope.Write(string.Empty, knownFields.Count - 1, bjo, ref entryWriter);
            }
            
            id = key ?? throw new InvalidParameterException("Cannot find any identifier of the document.");
            if (sourceDocumentId != null && knownFields.TryGetByFieldName(Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName, out var documentSourceField))
                scope.Write(string.Empty, documentSourceField.FieldId, sourceDocumentId.AsSpan(), ref entryWriter);
            
            scope.Write(string.Empty, 0, id.AsSpan(), ref entryWriter);
            fields = entryWriter.CurrentFieldCount();
            return entryWriter.Finish(out output, _indexImplicitNull);
        }
        catch
        {
            ResetEntriesWriter();
            throw;
        }
    }
}
