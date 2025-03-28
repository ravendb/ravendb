using System;
using System.Diagnostics;
using Amazon.SimpleNotificationService.Model;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Utils;
using Constants = Raven.Client.Constants;

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

    public AnonymousCoraxDocumentConverterBase(Index index, int numberOfBaseFields = 1, string keyFieldName = null, bool storeValue = false, bool canContainSourceDocumentId = false) : base(index, storeValue, indexImplicitNull: index.Configuration.IndexMissingFieldsAsNull, index.Configuration.IndexEmptyEntries, 1, keyFieldName, Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName, canContainSourceDocumentId)
    {
        _isMultiMap = index.IsMultiMap;
    }

    protected override bool SetDocumentFields<TBuilder>(LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext, TBuilder builder,
        object sourceDocument)
    {
        var boostedValue = doc as BoostedValue;
        var documentToProcess = boostedValue == null ? doc : boostedValue.Value;
        
        // It is important to note that as soon as an accessor is created this instance is tied to the underlying property type.
        // This optimization is not able to handle differences in types for the same property. Therefore, this instances cannot
        // be reused for Map and Reduce documents at the same time. You need a new instance to do so. 
        IPropertyAccessor accessor;
        if (_isMultiMap == false)
            accessor = _propertyAccessor ??= PropertyAccessor.Create(documentToProcess.GetType(), documentToProcess);
        else
            accessor = TypeConverter.GetPropertyAccessor(documentToProcess);

        var storedValue = _storeValue ? new DynamicJsonValue() : null;

        var knownFields = GetKnownFieldsForWriter();

        // We prepare for the next entry.
        if (boostedValue != null)
            builder.Boost(boostedValue.Boost);

        if (CompoundFields != null)
            HandleCompoundFields();

        bool hasFields = false;
        
        foreach (var property in accessor.GetProperties(documentToProcess))
        {
            var value = property.Value;

            if (_fields.TryGetValue(property.Key, out var field) == false)
                throw new InvalidOperationException($"Field '{property.Key}' is not defined. Available fields: {string.Join(", ", _fields.Keys)}.");

            InsertRegularField(field, value, indexContext, builder, sourceDocument, out var innerShouldSkip);

            if (innerShouldSkip)
                RegisterMissingFieldFor(field);
            
            hasFields |= innerShouldSkip == false;
                
            if (storedValue is not null && innerShouldSkip == false)
            {
                //Notice: we are always saving values inside Corax index. This method is explicitly for MapReduce because we have to have JSON as the last item.
                var blittableValue = TypeConverter.ToBlittableSupportedType(value, out TypeConverter.BlittableSupportedReturnType returnType, flattenArrays: true);

                if (returnType != TypeConverter.BlittableSupportedReturnType.Ignored)
                    storedValue[property.Key] = blittableValue;
            }
        }

        if (hasFields is false && _indexEmptyEntries is false)
            return false;

        if (storedValue is not null)
        {
            using var bjo = indexContext.ReadObject(storedValue, "corax field as json");
            builder.Store(bjo);
        }
            
        var id = key ?? throw new InvalidParameterException("Cannot find any identifier of the document.");
        if (sourceDocumentId != null && knownFields.TryGetByFieldName(Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName, out var documentSourceField))
            builder.Write(documentSourceField.FieldId, string.Empty, sourceDocumentId.AsSpan());

        builder.Write(0, string.Empty, id.AsSpan());
        
        WriteNonExistingMarkerForMissingFields(builder);

        return true;
        
        void HandleCompoundFields()
        {
            _compoundFieldsBuffer ??= new byte[128];
            // a bit yucky, but we put the compound fields in the end, and we need to account for the id() field, etc
            var baseLine = _index.Definition.IndexFields.Count - CompoundFields.Count + 1;
            for (int i = 0; i < CompoundFields.Count; i++)
            {
                var fields = CompoundFields[i];
                if (fields.Length != 2)
                    throw new NotSupportedInCoraxException("Currently compound indexes are only supporting exactly 2 fields");
                
                var firstValueLen = AppendFieldValue(fields[0], accessor.GetValue(fields[0], doc), 0, builder);
                var totalLen = AppendFieldValue(fields[1], accessor.GetValue(fields[1], doc), firstValueLen, builder);
                
                Debug.Assert(firstValueLen <= byte.MaxValue, "firstValueLen <= byte.MaxValue, checked in the AppendFieldValue already");
                Debug.Assert(totalLen < _compoundFieldsBuffer.Length, "totalLen < _compoundFieldsBuffer.Length, ensured by AppendFieldValue");
                _compoundFieldsBuffer[totalLen++] = (byte)firstValueLen;

                const int maxLength = global::Corax.Constants.Terms.MaxLength;
                if (totalLen > maxLength)
                    throw new ArgumentOutOfRangeException(
                        $"Compound Field total size cannot exceed {maxLength}, but was {totalLen} for {string.Join(", ", CompoundFields[i])}");
                
                var fieldId = baseLine + i;
                
                builder.Write( fieldId, _compoundFieldsBuffer.AsSpan()[..totalLen]);
            }

            if (_compoundFieldsBuffer.Length > 64 * 1024)
            {
                // let's ensure that we aren't holding on to really big buffers forever
                // in general, the limit is 256 per term with 2 max, so unlikely to hit that, but 
                // to be future-proof..
                _compoundFieldsBuffer = null;
            }
        }
    }
}
