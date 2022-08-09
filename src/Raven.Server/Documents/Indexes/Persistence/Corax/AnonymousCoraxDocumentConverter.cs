using System;
using System.Collections.Generic;
using Amazon.SimpleNotificationService.Model;
using Corax;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Utils;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class AnonymousCoraxDocumentConverter : CoraxDocumentConverterBase
{
    private readonly bool _isMultiMap;
    private IPropertyAccessor _propertyAccessor;

    public AnonymousCoraxDocumentConverter(Index index, bool storeValue = false)
        : base(index, storeValue, index.Configuration.IndexEmptyEntries, true, 1, null, Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
    {
        _isMultiMap = index.IsMultiMap;
    }

    public override Span<byte> SetDocumentFields(LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext,
        out LazyStringValue id, Span<byte> writerBuffer)
    {
        var knownFields = GetKnownFieldsForWriter();
        var boostedValue = doc as BoostedValue;
        var documentToProcess = boostedValue == null ? doc : boostedValue.Value;
        id = default;

        IPropertyAccessor accessor;

        if (_isMultiMap == false)
            accessor = _propertyAccessor ??= PropertyAccessor.Create(documentToProcess.GetType(), documentToProcess);
        else
            accessor = TypeConverter.GetPropertyAccessor(documentToProcess);

        // todo maciej
        // We need to discuss how we will handle this.  
        // https://github.com/ravendb/ravendb/pull/13730#discussion_r820661488
        var entryWriter = new IndexEntryWriter(writerBuffer, knownFields);

        var scope = new SingleEntryWriterScope(_allocator);
        var storedValue = _storeValue ? new DynamicJsonValue() : null;

        foreach (var property in accessor.GetPropertiesInOrder(documentToProcess))
        {
            var value = property.Value;

            if(_fields.TryGetValue(property.Key, out var field)== false)
                throw new InvalidOperationException($"Field '{property.Key}' is not defined. Available fields: {string.Join(", ", _fields.Keys)}.");

            if (storedValue is not null)
            {
                //Notice: we are always saving values inside Corax index. This method is explicitly for MapReduce because we have to have JSON as the last item.
                var blittableValue = TypeConverter.ToBlittableSupportedType(value, out TypeConverter.BlittableSupportedReturnType returnType, flattenArrays: true);

                if (returnType == TypeConverter.BlittableSupportedReturnType.Ignored)
                    continue;

                storedValue[property.Key] = blittableValue;
            }

            InsertRegularField(field, value, indexContext, ref entryWriter, scope);
        }

        if (storedValue is not null)
        {
            var bjo = indexContext.ReadObject(storedValue, "corax field as json");
            scope.Write(knownFields.Count - 1, bjo, ref entryWriter);
        }

        if (entryWriter.IsEmpty() == true)
            return Span<byte>.Empty;
        
        id = key ?? (sourceDocumentId ?? throw new InvalidParameterException("Cannot find any identifier of the document."));
        scope.Write(0, id.AsSpan(), ref entryWriter);
        entryWriter.Finish(out var output);

        return output;
    }
}
