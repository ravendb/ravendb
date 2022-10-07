using System;
using System.Collections.Generic;
using System.IO;
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

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class JintCoraxDocumentConverter : JintCoraxDocumentConverterBase
{
    private readonly IndexFieldOptions _allFields;

    public JintCoraxDocumentConverter(MapIndex index, bool storeValue = false)
        : base(index, storeValue, false, true, 1, null, Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
    {
        index.Definition.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out _allFields);
    }

    public JintCoraxDocumentConverter(MapReduceIndex index, bool storeValue = false)
        : base(index, storeValue, false, true, 1, null, Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
    {
        index.Definition.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out _allFields);
    }
    
    public override ByteStringContext<ByteStringMemoryCache>.InternalScope SetDocumentFields(
        LazyStringValue key, LazyStringValue sourceDocumentId,
        object doc, JsonOperationContext indexContext, out LazyStringValue id,
        out ByteString output)
    {
        // We prepare for the next entry.
        ref var entryWriter = ref GetEntriesWriter();
        if (doc is not ObjectInstance documentToProcess)
        {
            //nothing to index, finish the job
            id = null;
            entryWriter.Finish(out output);
            return default;
        }

        id = key ?? (sourceDocumentId ?? throw new InvalidDataException("Cannot find any identifier of the document."));
        var singleEntryWriterScope = new SingleEntryWriterScope(_allocator);

        if (TryGetBoostedValue(documentToProcess, out var boostedValue, out var documentBoost))
            ThrowWhenBoostingIsInDocument();

        //Write id/key
        singleEntryWriterScope.Write(string.Empty, 0, id.AsSpan(), ref entryWriter);
        var indexingScope = CurrentIndexingScope.Current;
        foreach (var (property, propertyDescriptor) in documentToProcess.GetOwnProperties())
        {
            var propertyAsString = property.AsString();
            var field = GetFieldObjectForProcessing(propertyAsString);
            var isDynamicFieldEnumerable = IsDynamicFieldEnumerable(propertyDescriptor.Value, propertyAsString, field, out var iterator);
            bool shouldSaveAsBlittable;
            object value;
            JsValue actualValue;

            if (isDynamicFieldEnumerable)
            {
                var enumerableScope = CreateEnumerableWriterScope();
                enumerableScope.SetAsDynamic();
                do
                {
                    ProcessObject(iterator.Current, propertyAsString, field, ref entryWriter, enumerableScope, out shouldSaveAsBlittable, out value, out actualValue);
                    if (shouldSaveAsBlittable)
                        ProcessAsJson(actualValue, field, ref entryWriter, enumerableScope);

                    var disposable = value as IDisposable;
                    disposable?.Dispose();
                    
                } while (iterator.MoveNext());
                
                enumerableScope.Finish(field.Name, field.Id, ref entryWriter);
            }
            else
            {
                ProcessObject(propertyDescriptor.Value, propertyAsString, field, ref entryWriter, singleEntryWriterScope, out shouldSaveAsBlittable, out value, out actualValue);
                if (shouldSaveAsBlittable)
                    ProcessAsJson(actualValue, field, ref entryWriter, singleEntryWriterScope);
                var disposable = value as IDisposable;
                disposable?.Dispose();
            }
            
        }


        if (_storeValue)
        {
            //Write __stored_fields at the end of entry...
            StoreValue(ref entryWriter, singleEntryWriterScope);
        }

        return entryWriter.Finish(out output);


        //Helpers
        
        void ProcessAsJson(JsValue actualValue, IndexField field, ref CoraxLib.IndexEntryWriter entryWriter, IWriterScope writerScope)
        {
            var value = TypeConverter.ToBlittableSupportedType(actualValue, flattenArrays: false, forIndexing: true, engine: documentToProcess.Engine,
                context: indexContext);
            InsertRegularField(field, value, indexContext, ref entryWriter, writerScope);
        }
        
        static bool TryGetBoostedValue(ObjectInstance valueToCheck, out JsValue value, out float? boost)
        {
            value = JsValue.Undefined;
            boost = null;

            if (valueToCheck.TryGetValue(BoostPropertyName, out var boostValue) == false)
                return false;

            if (valueToCheck.TryGetValue(ValuePropertyName, out var valueValue) == false)
                return false;

            if (boostValue.IsNumber() == false)
                return false;

            boost = (float)boostValue.AsNumber();
            value = valueValue;

            ThrowWhenBoostingIsInDocument();
            return false;
        }

        static bool IsObject(JsValue value)
        {
            return value.IsObject() && value.IsArray() == false;
        }
        
        void ProcessObject(JsValue valueToInsert, in string propertyAsString, IndexField field, ref CoraxLib.IndexEntryWriter entryWriter, IWriterScope writerScope, out bool shouldProcessAsBlittable, out object value, out JsValue actualValue)
            {
                value = null;
                actualValue = valueToInsert;
                var isObject = IsObject(actualValue);
                if (isObject)
                {
                    if (TryGetBoostedValue(actualValue.AsObject(), out _, out _))
                    { //todo leftover to implement boosting inside index someday, now it will throw
                    }

                    //In case TryDetectDynamicFieldCreation finds a dynamic field it will populate 'field.Name' with the actual property name
                    //so we must use field.Name and not property from this point on.
                    var val = TryDetectDynamicFieldCreation(propertyAsString, actualValue.AsObject(), field, indexingScope);
                    if (val != null)
                    {
                        if (val.IsObject() && val.AsObject().TryGetValue(SpatialPropertyName, out _))
                        {
                            actualValue = val; //Here we populate the dynamic spatial field that will be handled below.
                        }
                        else
                        {
                            value = TypeConverter.ToBlittableSupportedType(val, flattenArrays: false, forIndexing: true, engine: documentToProcess.Engine,
                                context: indexContext);

                            InsertRegularField(field, value, indexContext, ref entryWriter, writerScope);

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
                    if (objectValue.HasOwnProperty(SpatialPropertyName) && objectValue.TryGetValue(SpatialPropertyName, out var inner))
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

                        InsertRegularField(field, spatial, indexContext, ref entryWriter, writerScope);

                        shouldProcessAsBlittable = false;
                        return;
                    }
                }

                shouldProcessAsBlittable = true;
            }
        
        
        IndexField GetFieldObjectForProcessing(in string propertyAsString)
        {
            if (_fields.TryGetValue(propertyAsString, out var field) == false)
            {
                int currentId = CoraxLib.Constants.IndexWriter.DynamicField;
                if (_knownFieldsForWriter.TryGetByFieldName(propertyAsString, out var binding))
                    currentId = binding.FieldId;

                field = _fields[propertyAsString] = IndexField.Create(propertyAsString, new IndexFieldOptions(), _allFields, currentId);
                indexingScope.DynamicFields ??= new();
                indexingScope.DynamicFields[propertyAsString] = field.Indexing;
                indexingScope.CreatedFieldsCount++;
            }

            return field;
        }
        
        bool IsDynamicFieldEnumerable(JsValue propertyDescriptorValue, string propertyAsString, IndexField field, out IEnumerator<JsValue> iterator)
        {
            iterator = Enumerable.Empty<JsValue>().GetEnumerator();

            if (propertyDescriptorValue.IsArray() == false)
                return false;

            iterator = propertyDescriptorValue.AsArray().GetEnumerator();
            if (iterator.MoveNext() == false || iterator.Current is null || iterator.Current.IsObject() == false || iterator.Current.IsArray() == true)
                return false;

            var valueAsObject = iterator.Current.AsObject();

            return TryDetectDynamicFieldCreation(propertyAsString, valueAsObject, field, indexingScope) is not null
                   || valueAsObject.HasOwnProperty(SpatialPropertyName);
        }

        void StoreValue(ref CoraxLib.IndexEntryWriter entryWriter, SingleEntryWriterScope scope)
        {
            var storedValue = JsBlittableBridge.Translate(indexContext, documentToProcess.Engine, documentToProcess);
            unsafe
            {
                using (_allocator.Allocate(storedValue.Size, out Span<byte> blittableBuffer))
                {
                    fixed (byte* bPtr = blittableBuffer)
                        storedValue.CopyTo(bPtr);

                    scope.Write(string.Empty, GetKnownFieldsForWriter().Count - 1, blittableBuffer, ref entryWriter);
                }
            }
        }
    }
}

public abstract class JintCoraxDocumentConverterBase : CoraxDocumentConverterBase
{
    protected const string ValuePropertyName = "$value";
    protected const string OptionsPropertyName = "$options";
    protected const string NamePropertyName = "$name";
    protected const string SpatialPropertyName = "$spatial";
    protected const string BoostPropertyName = "$boost";

    protected JintCoraxDocumentConverterBase(Index index, bool storeValue, bool indexImplicitNull, bool indexEmptyEntries, int numberOfBaseFields,
        string keyFieldName,
        string storeValueFieldName, ICollection<IndexField> fields = null) : base(index, storeValue, indexImplicitNull, indexEmptyEntries, numberOfBaseFields,
        keyFieldName, storeValueFieldName, fields)
    {
    }

    protected static JsValue TryDetectDynamicFieldCreation(string property, ObjectInstance valueAsObject, IndexField field, CurrentIndexingScope scope)
    {
        //We have a field creation here _ = {"$value":val, "$name","$options":{...}}
        if (!valueAsObject.HasOwnProperty(ValuePropertyName))
            return null;

        var value = valueAsObject.GetOwnProperty(ValuePropertyName).Value;
        PropertyDescriptor nameProperty = valueAsObject.GetOwnProperty(NamePropertyName);
        if (nameProperty != null)
        {
            var fieldNameObj = nameProperty.Value;
            if (fieldNameObj.IsString() == false)
                throw new ArgumentException($"Dynamic field {property} is expected to have a string {NamePropertyName} property but got {fieldNameObj}");

            field.Name = fieldNameObj.AsString();
            field.Id = CoraxLib.Constants.IndexWriter.DynamicField;
        }
        else
        {
            field.Name = property;
        }

        if (valueAsObject.HasOwnProperty(OptionsPropertyName))
        {
            var options = valueAsObject.GetOwnProperty(OptionsPropertyName).Value;
            if (options.IsObject() == false)
            {
                throw new ArgumentException($"Dynamic field {property} is expected to contain an object with three properties " +
                                            $"{OptionsPropertyName}, {NamePropertyName} and {OptionsPropertyName} the later should be a valid IndexFieldOptions object.");
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

    protected static void ThrowWhenBoostingIsInDocument()
    {
        throw new NotImplementedInCoraxException("Indexing-time boosting is not implemented.");
    }

    public override void Dispose()
    {
        base.Dispose();
    }
}
