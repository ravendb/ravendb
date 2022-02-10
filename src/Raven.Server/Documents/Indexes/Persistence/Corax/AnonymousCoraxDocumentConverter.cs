using System;
using System.Collections.Generic;
using System.Text;
using Amazon.SimpleNotificationService.Model;
using Corax;
using NuGet.Protocol;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Raven.Server.Utils;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class AnonymousCoraxDocumentConverter : CoraxDocumentConverterBase
{
    private readonly bool _isMultiMap;
    private IPropertyAccessor _propertyAccessor;

    public AnonymousCoraxDocumentConverter(Index index, bool storeValue = false)
        : base(index, storeValue, false, true, 1, null,Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
    {
        _isMultiMap = index.IsMultiMap;
    }

    public override void Dispose()
    {
        //throw new NotImplementedException();
    }

    public override Span<byte> SetDocumentFields(LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext,
        out LazyStringValue id)
    {
        var boostedValue = doc as BoostedValue;
        var documentToProcess = boostedValue == null ? doc : boostedValue.Value;

        IPropertyAccessor accessor;

        if (_isMultiMap == false)
            accessor = _propertyAccessor ??= PropertyAccessor.Create(documentToProcess.GetType(), documentToProcess);
        else
            accessor = Raven.Server.Utils.TypeConverter.GetPropertyAccessor(documentToProcess);

        using var _ = _allocator.Allocate(9086 * 8, out ByteString buffer);
        var entryWriter = new IndexEntryWriter(buffer.ToSpan(), _knownFields);
        id = key ?? (sourceDocumentId ?? throw new InvalidParameterException(""));
        List<int> stringsLength = new List<int>(128);
        var scope = new SingleEntryWriterScope(stringsLength, _allocator);

        var storedValue = _storeValue ? new DynamicJsonValue() : null;


        scope.Write(0, id.AsSpan(), ref entryWriter);
        int idX = 1;
        foreach (var property in accessor.GetPropertiesInOrder(documentToProcess))
        {
            var value = property.Value;

            IndexField field;

            try
            {
                field = _fields[property.Key];
                field.Id = idX++;
            }
            catch (KeyNotFoundException e)
            {
                throw new InvalidOperationException($"Field '{property.Key}' is not defined. Available fields: {string.Join(", ", _fields.Keys)}.", e);
            }

            if (storedValue is not null)
            {
                var blittableValue = TypeConverter.ToBlittableSupportedType(value, out TypeConverter.BlittableSupportedReturnType returnType, flattenArrays: true);

                if (returnType == TypeConverter.BlittableSupportedReturnType.Ignored)
                    continue;

                storedValue[property.Key] = blittableValue;
            }

            InsertRegularField(field, value, indexContext, out var shouldSkip, ref entryWriter, scope);
        }

        if (storedValue is not null)
        {
            var bjo = indexContext.ReadObject(storedValue, "corax field as json");
            unsafe
            {
                using (_allocator.Allocate(bjo.Size, out Span<byte> blittableBuffer))
                {
                    fixed (byte* bPtr = blittableBuffer)
                        bjo.CopyTo(bPtr);

                    scope.Write(_knownFields.Count - 1, blittableBuffer, ref entryWriter);
                }
            }
        }

        entryWriter.Finish(out var output);
        return output;
    }
}
