using System;
using System.Collections.Generic;
using System.Linq;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Patch;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server;
using CoraxLib = global::Corax;
using JavaScriptFieldName = Raven.Client.Constants.Documents.Indexing.Fields.JavaScript;
namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public sealed class CoraxJintDocumentConverter : CoraxJintDocumentConverterBase
{
    public CoraxJintDocumentConverter(MapIndex index, bool storeValue = false) : base(index, index.Definition.IndexDefinition, storeValue, 1, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName, canContainSourceDocumentId: true) 
    {
    }

    public CoraxJintDocumentConverter(MapReduceIndex index, bool storeValue = false) : base(index, index.Definition.IndexDefinition, storeValue, 1, keyFieldName: Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName, storeValueFieldName: Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName, canContainSourceDocumentId: true) 
    {
    }
}

public abstract class CoraxJintDocumentConverterBase : CoraxDocumentConverterBase
{
    private readonly IndexFieldOptions _allFields;


    protected CoraxJintDocumentConverterBase(Index index, IndexDefinition definition, bool storeValue, int numberOfBaseFields, string keyFieldName,
        string storeValueFieldName, ICollection<IndexField> fields = null, bool canContainSourceDocumentId = false) :
        base(index, storeValue, index.Configuration.IndexMissingFieldsAsNull, index.Configuration.IndexEmptyEntries, numberOfBaseFields, keyFieldName, storeValueFieldName, canContainSourceDocumentId, fields) //todo
    {
        definition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out _allFields);
    }

    public override ByteStringContext<ByteStringMemoryCache>.InternalScope SetDocumentFields(LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext, out LazyStringValue id, out ByteString output, out float? documentBoost, out int fields)
    {
        documentBoost = null;
        // We prepare for the next entry.
        ref var entryWriter = ref GetEntriesWriter();
        var fieldMapping = GetKnownFieldsForWriter();

        try
        {
            if (doc is not ObjectInstance documentToProcess)
            {
                //nothing to index, finish the job
                id = null;
                entryWriter.Finish(out output, _indexEmptyEntries);
                fields = 0;
                return default;
            }

            var singleEntryWriterScope = new SingleEntryWriterScope(Allocator);
            id = key;

            if (key != null)
                singleEntryWriterScope.Write(string.Empty, 0, key.AsSpan(), ref entryWriter);

            if (sourceDocumentId != null && fieldMapping.TryGetByFieldName(Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName, out var keyBinding))
                singleEntryWriterScope.Write(string.Empty, keyBinding.FieldId, sourceDocumentId.AsSpan(), ref entryWriter);

            if (TryGetBoostedValue(documentToProcess, out var boostedValue, out documentBoost))
                documentToProcess = boostedValue.AsObject();

            var indexingScope = CurrentIndexingScope.Current;
            foreach (var (property, propertyDescriptor) in documentToProcess.GetOwnProperties())
            {
                var propertyAsString = property.AsString();
                var field = GetFieldObjectForProcessing(propertyAsString, indexingScope);
                var isDynamicFieldEnumerable = IsDynamicFieldEnumerable(propertyDescriptor.Value, propertyAsString, field, indexingScope, out var iterator);
                bool shouldSaveAsBlittable;
                object value;
                JsValue actualValue;
                if (isDynamicFieldEnumerable)
                {
                    var enumerableScope = CreateEnumerableWriterScope();
                    enumerableScope.SetAsDynamic();
                    do
                    {
                        ProcessObject(iterator.Current, propertyAsString, field, ref entryWriter, enumerableScope, indexingScope, documentToProcess,
                            out shouldSaveAsBlittable, out value, out actualValue, out _);
                        if (shouldSaveAsBlittable)
                            ProcessAsJson(actualValue, field, ref entryWriter, enumerableScope, documentToProcess, out _);

                        var disposable = value as IDisposable;
                        disposable?.Dispose();
                    } while (iterator.MoveNext());

                    enumerableScope.Finish(field.Name, field.Id, ref entryWriter);
                }
                else
                {
                    ProcessObject(propertyDescriptor.Value, propertyAsString, field, ref entryWriter, singleEntryWriterScope, indexingScope, documentToProcess,
                        out shouldSaveAsBlittable, out value, out actualValue, out _);
                    if (shouldSaveAsBlittable)
                        ProcessAsJson(actualValue, field, ref entryWriter, singleEntryWriterScope, documentToProcess, out _);
                    var disposable = value as IDisposable;
                    disposable?.Dispose();
                }
            }

            if (_storeValue)
            {
                //Write __stored_fields at the end of entry...
                StoreValue(indexContext, ref entryWriter, singleEntryWriterScope, documentToProcess);
            }

            fields = entryWriter.CurrentFieldCount();
            return entryWriter.Finish(out output, _indexEmptyEntries);
        }
        catch
        {
            ResetEntriesWriter();
            throw;
        }

        //Helpers

        void ProcessAsJson(JsValue actualValue, IndexField field, ref CoraxLib.IndexEntryWriter entryWriter, IWriterScope writerScope, ObjectInstance documentToProcess, out bool shouldSkip)
        {
            var value = TypeConverter.ToBlittableSupportedType(actualValue, flattenArrays: false, forIndexing: true, engine: documentToProcess.Engine,
                context: indexContext);
            InsertRegularField(field, value, indexContext, ref entryWriter, writerScope, out shouldSkip);
        }

        static bool TryGetBoostedValue(ObjectInstance valueToCheck, out JsValue value, out float? boost)
        {
            value = JsValue.Undefined;
            boost = null;

            if (valueToCheck.TryGetValue(JavaScriptFieldName.BoostPropertyName, out var boostValue) == false)
                return false;

            if (valueToCheck.TryGetValue(JavaScriptFieldName.ValuePropertyName, out var valueValue) == false)
                return false;

            if (boostValue.IsNumber() == false)
                return false;

            boost = (float)boostValue.AsNumber();
            value = valueValue;

            return true;
        }

        static bool IsObject(JsValue value)
        {
            return value.IsObject() && value.IsArray() == false;
        }

        void ProcessObject(JsValue valueToInsert, in string propertyAsString, IndexField field, ref CoraxLib.IndexEntryWriter entryWriter, IWriterScope writerScope,
            CurrentIndexingScope indexingScope, ObjectInstance documentToProcess, out bool shouldProcessAsBlittable, out object value, out JsValue actualValue, out bool shouldSkip)
        {
            shouldSkip = false;
            value = null;
            actualValue = valueToInsert;
            var isObject = IsObject(actualValue);
            if (isObject)
            {
                if (TryGetBoostedValue(actualValue.AsObject(), out _, out _))
                {
                    ThrowWhenBoostingIsInDocument();
                }

                //In case TryDetectDynamicFieldCreation finds a dynamic field it will populate 'field.Name' with the actual property name
                //so we must use field.Name and not property from this point on.
                var val = TryDetectDynamicFieldCreation(propertyAsString, actualValue.AsObject(), field, indexingScope);
                if (val != null)
                {
                    if (val.IsObject() && val.AsObject().TryGetValue(JavaScriptFieldName.SpatialPropertyName, out _))
                    {
                        actualValue = val; //Here we populate the dynamic spatial field that will be handled below.
                    }
                    else
                    {
                        value = TypeConverter.ToBlittableSupportedType(val, flattenArrays: false, forIndexing: true, engine: documentToProcess.Engine,
                            context: indexContext);

                        InsertRegularField(field, value, indexContext, ref entryWriter, writerScope, out shouldSkip);

                        if (value is IDisposable toDispose1)
                        {
                            // the value was converted to a corax field and isn't needed anymore
                            toDispose1.Dispose();
                        }

                        shouldProcessAsBlittable = false;
                        return;
                    }
                }

                var objectValue = actualValue.AsObject();
                if (objectValue.HasOwnProperty(JavaScriptFieldName.SpatialPropertyName) && objectValue.TryGetValue(JavaScriptFieldName.SpatialPropertyName, out var inner))
                {
                    SpatialField spatialField;
                    IEnumerable<object> spatial;
                    if (inner.IsString())
                    {
                        spatialField = AbstractStaticIndexBase.GetOrCreateSpatialField(field.Name);
                        spatial = AbstractStaticIndexBase.CreateSpatialField(spatialField, inner.AsString());
                    }
                    else if (inner.IsObject())
                    {
                        var innerObject = inner.AsObject();
                        if (innerObject.HasOwnProperty("Lat") && innerObject.HasOwnProperty("Lng") && innerObject.TryGetValue("Lat", out var lat)
                            && lat.IsNumber() && innerObject.TryGetValue("Lng", out var lng) && lng.IsNumber())
                        {
                            spatialField = AbstractStaticIndexBase.GetOrCreateSpatialField(field.Name);
                            spatial = AbstractStaticIndexBase.CreateSpatialField(spatialField, lat.AsNumber(), lng.AsNumber());
                        }
                        else
                        {
                            shouldProcessAsBlittable = false;
                            return; //Ignoring bad spatial field
                        }
                    }
                    else
                    {
                        shouldProcessAsBlittable = false;
                        return; //Ignoring bad spatial field
                    }

                    InsertRegularField(field, spatial, indexContext, ref entryWriter, writerScope, out shouldSkip);

                    shouldProcessAsBlittable = false;
                    return;
                }
            }

            shouldProcessAsBlittable = true;
        }
    }

    private IndexField GetFieldObjectForProcessing(in string propertyAsString, CurrentIndexingScope indexingScope)
    {
        if (_fields.TryGetValue(propertyAsString, out var field) == false)
        {
            int currentId = CoraxLib.Constants.IndexWriter.DynamicField;
            if (KnownFieldsForWriter.TryGetByFieldName(Allocator, propertyAsString, out var binding))
                currentId = binding.FieldId;

            field = _fields[propertyAsString] = IndexField.Create(propertyAsString, new IndexFieldOptions(), _allFields, currentId);
            indexingScope.DynamicFields ??= new();
            indexingScope.DynamicFields[propertyAsString] = field.Indexing;
            indexingScope.CreatedFieldsCount++;
        }

        return field;
    }

    private bool IsDynamicFieldEnumerable(JsValue propertyDescriptorValue, string propertyAsString, IndexField field, CurrentIndexingScope indexingScope,
        out IEnumerator<JsValue> iterator)
    {
        iterator = Enumerable.Empty<JsValue>().GetEnumerator();

        if (propertyDescriptorValue.IsArray() == false)
            return false;

        iterator = propertyDescriptorValue.AsArray().GetEnumerator();
        if (iterator.MoveNext() == false || iterator.Current is null || iterator.Current.IsObject() == false || iterator.Current.IsArray() == true)
            return false;

        var valueAsObject = iterator.Current.AsObject();

        return TryDetectDynamicFieldCreation(propertyAsString, valueAsObject, field, indexingScope) is not null
               || valueAsObject.HasOwnProperty(JavaScriptFieldName.SpatialPropertyName);
    }

    private void StoreValue(JsonOperationContext indexContext, ref CoraxLib.IndexEntryWriter entryWriter, SingleEntryWriterScope scope, ObjectInstance documentToProcess)
    {
        var storedValue = JsBlittableBridge.Translate(indexContext, documentToProcess.Engine, documentToProcess);
        unsafe
        {
            using (Allocator.Allocate(storedValue.Size, out Span<byte> blittableBuffer))
            {
                fixed (byte* bPtr = blittableBuffer)
                    storedValue.CopyTo(bPtr);

                scope.Write(string.Empty, GetKnownFieldsForWriter().StoredJsonPropertyOffset, blittableBuffer, ref entryWriter);
            }
        }
    }

    private static JsValue TryDetectDynamicFieldCreation(string property, ObjectInstance valueAsObject, IndexField field, CurrentIndexingScope scope)
    {
        //We have a field creation here _ = {"$value":val, "$name","$options":{...}}
        if (!valueAsObject.HasOwnProperty(JavaScriptFieldName.ValuePropertyName))
            return null;

        var value = valueAsObject.GetOwnProperty(JavaScriptFieldName.ValuePropertyName).Value;
        PropertyDescriptor nameProperty = valueAsObject.GetOwnProperty(JavaScriptFieldName.NamePropertyName);
        if (nameProperty != null)
        {
            var fieldNameObj = nameProperty.Value;
            if (fieldNameObj.IsString() == false)
                throw new ArgumentException($"Dynamic field {property} is expected to have a string {JavaScriptFieldName.NamePropertyName} property but got {fieldNameObj}");

            field.Name = fieldNameObj.AsString();
            field.Id = CoraxLib.Constants.IndexWriter.DynamicField;
        }
        else
        {
            field.Name = property;
        }

        if (valueAsObject.HasOwnProperty(JavaScriptFieldName.OptionsPropertyName))
        {
            var options = valueAsObject.GetOwnProperty(JavaScriptFieldName.OptionsPropertyName).Value;
            if (options.IsObject() == false)
            {
                throw new ArgumentException($"Dynamic field {property} is expected to contain an object with three properties " +
                                            $"{JavaScriptFieldName.OptionsPropertyName}, {JavaScriptFieldName.NamePropertyName} and {JavaScriptFieldName.OptionsPropertyName} the later should be a valid IndexFieldOptions object.");
            }

            var optionObj = options.AsObject();
            foreach (var kvp in optionObj.GetOwnProperties())
            {
                var optionValue = kvp.Value.Value;
                if (optionValue.IsUndefined() || optionValue.IsNull())
                    continue;

                var propertyName = kvp.Key;
                var propertyNameAsString = propertyName.AsString();
                if (string.Equals(propertyNameAsString, nameof(CreateFieldOptions.Indexing), StringComparison.OrdinalIgnoreCase))
                {
                    field.Indexing = GetEnum<FieldIndexing>(optionValue, propertyNameAsString);

                    continue;
                }

                if (string.Equals(propertyNameAsString, nameof(CreateFieldOptions.Storage), StringComparison.OrdinalIgnoreCase))
                {
                    if (optionValue.IsBoolean())
                        field.Storage = optionValue.AsBoolean()
                            ? FieldStorage.Yes
                            : FieldStorage.No;
                    else
                        field.Storage = GetEnum<FieldStorage>(optionValue, propertyNameAsString);

                    continue;
                }

                if (string.Equals(propertyNameAsString, nameof(CreateFieldOptions.TermVector), StringComparison.OrdinalIgnoreCase))
                {
                    field.TermVector = GetEnum<FieldTermVector>(optionValue, propertyNameAsString);

                    continue;
                }
            }
        }

        if (scope.DynamicFields.TryGetValue(field.Name, out _) == false)
        {
            scope.DynamicFields[field.Name] = field.Indexing;
            scope.CreatedFieldsCount++;
        }

        return value;
        
        TEnum GetEnum<TEnum>(JsValue optionValue, string propertyName)
        {
            if (optionValue.IsString() == false)
                throw new ArgumentException($"Could not parse dynamic field option property '{propertyName}' value ('{optionValue}') because it is not a string.");

            var optionValueAsString = optionValue.AsString();
            if (Enum.TryParse(typeof(TEnum), optionValueAsString, true, out var enumValue) == false)
                throw new ArgumentException(
                    $"Could not parse dynamic field option property '{propertyName}' value ('{optionValueAsString}') into '{typeof(TEnum).Name}' enum.");

            return (TEnum)enumValue;
        }
    }

    private static void ThrowWhenBoostingIsInDocument()
    {
        throw new NotImplementedInCoraxException("Indexing-time boosting of field is not implemented.");
    }
}
