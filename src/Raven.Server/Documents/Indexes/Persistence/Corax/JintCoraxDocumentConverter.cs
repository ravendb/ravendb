using System;
using System.Collections.Generic;
using System.IO;
using Amazon.SimpleNotificationService.Model;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
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

    //todo maciej: refactor | stop duplicating code from LuceneJint[...] https://github.com/ravendb/ravendb/pull/13730#discussion_r825928762
    public override Span<byte> SetDocumentFields(LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext,
        out LazyStringValue id, Span<byte> writerBuffer)
    {
        if (doc is not ObjectInstance documentToProcess)
        {
            id = null;
            return Span<byte>.Empty;
        }

        var entryWriter = new CoraxLib.IndexEntryWriter(writerBuffer, _knownFields);

        id = key ?? (sourceDocumentId ?? throw new InvalidParameterException("Cannot find any identifier of the document."));
        var scope = new SingleEntryWriterScope(_allocator);


        if (TryGetBoostedValue(documentToProcess, out var boostedValue, out var documentBoost))
        {
            throw new InvalidDataException("Corax indexes doesn't support boosting inside. If you want to use boosting you have to do this in query!");
        }

        scope.Write(0, id.AsSpan(), ref entryWriter);
        int idX = 1;
        foreach (var (property, propertyDescriptor) in documentToProcess.GetOwnProperties())
        {
            var propertyAsString = property.AsString();

            if (_fields.TryGetValue(propertyAsString, out var field) == false)
            {
                field = _fields[propertyAsString] = IndexField.Create(propertyAsString, new IndexFieldOptions(), _allFields);
                field.Id = idX++;
            }

            object value;
            float? propertyBoost = null;
            var actualValue = propertyDescriptor.Value;
            var isObject = IsObject(actualValue);
            if (isObject)
            {
                if (TryGetBoostedValue(actualValue.AsObject(), out boostedValue, out propertyBoost))
                {
                    throw new InvalidDataException("Corax indexes doesn't support boosting inside. If you want to use boosting you have to do this in query!");
                }

                if (isObject)
                {
                    //In case TryDetectDynamicFieldCreation finds a dynamic field it will populate 'field.Name' with the actual property name
                    //so we must use field.Name and not property from this point on.
                    var val = TryDetectDynamicFieldCreation(propertyAsString, actualValue.AsObject(), field);
                    if (val != null)
                    {
                        if (val.IsObject() && val.AsObject().TryGetValue(SpatialPropertyName, out var _))
                        {
                            actualValue = val; //Here we populate the dynamic spatial field that will be handled below.
                        }
                        else
                        {
                            value = Utils.TypeConverter.ToBlittableSupportedType(val, flattenArrays: false, forIndexing: true, engine: documentToProcess.Engine,
                                context: indexContext);
                            InsertRegularField(field, value, indexContext, ref entryWriter, scope);
                            if (value is IDisposable toDispose1)
                            {
                                // the value was converted to a lucene field and isn't needed anymore
                                toDispose1.Dispose();
                            }

                            continue;
                        }
                    }

                    var objectValue = actualValue.AsObject();
                    if (objectValue.HasOwnProperty(SpatialPropertyName) && objectValue.TryGetValue(SpatialPropertyName, out var inner))
                    {
                        // This is raw code for spatial from LuceneConverter. Leftover as todo maciej

                        // SpatialField spatialField;
                        // IEnumerable<AbstractField> spatial;
                        // if (inner.IsString())
                        // {
                        //     spatialField = AbstractStaticIndexBase.GetOrCreateSpatialField(field.Name);
                        //     spatial = AbstractStaticIndexBase.CreateSpatialField(spatialField, inner.AsString());
                        // }
                        // else if (inner.IsObject())
                        // {
                        //     var innerObject = inner.AsObject();
                        //     if (innerObject.HasOwnProperty("Lat") && innerObject.HasOwnProperty("Lng") && innerObject.TryGetValue("Lat", out var lat)
                        //         && lat.IsNumber() && innerObject.TryGetValue("Lng", out var lng) && lng.IsNumber())
                        //     {
                        //         spatialField = AbstractStaticIndexBase.GetOrCreateSpatialField(field.Name);
                        //         spatial = AbstractStaticIndexBase.CreateSpatialField(spatialField, lat.AsNumber(), lng.AsNumber());
                        //     }
                        //     else
                        //     {
                        //         continue; //Ignoring bad spatial field
                        //     }
                        // }
                        // else
                        // {
                        //     continue; //Ignoring bad spatial field
                        // }
                        //
                        // numberOfCreatedFields = GetRegularFields(instance, field, CreateValueForIndexing(spatial, propertyBoost), indexContext, out _);
                        //
                        // newFields += numberOfCreatedFields;
                        //
                        // BoostDocument(instance, numberOfCreatedFields, documentBoost);
                        //
                        // continue;
                    }
                }
            }

            value = Utils.TypeConverter.ToBlittableSupportedType(actualValue, flattenArrays: false, forIndexing: true, engine: documentToProcess.Engine,
                context: indexContext);
            InsertRegularField(field, value, indexContext, ref entryWriter, scope);

            if (value is IDisposable toDispose)
            {
                // the value was converted to a lucene field and isn't needed anymore
                toDispose.Dispose();
            }
        }


        if (_storeValue)
        {
            var storedValue = JsBlittableBridge.Translate(indexContext,
                documentToProcess.Engine,
                documentToProcess);
            unsafe
            {
                using (_allocator.Allocate(storedValue.Size, out Span<byte> blittableBuffer))
                {
                    fixed (byte* bPtr = blittableBuffer)
                        storedValue.CopyTo(bPtr);

                    scope.Write(_knownFields.Count - 1, blittableBuffer, ref entryWriter);
                }
            }
        }

        entryWriter.Finish(out var output);
        return output;

        static bool IsObject(JsValue value)
        {
            return value.IsObject() && value.IsArray() == false;
        }
    }

    public override void Dispose()
    {
        throw new NotImplementedException();
    }
}

public abstract class JintCoraxDocumentConverterBase : CoraxDocumentConverterBase
{
    protected const string ValuePropertyName = "$value";
    protected const string OptionsPropertyName = "$options";
    protected const string NamePropertyName = "$name";
    protected const string SpatialPropertyName = "$spatial";
    protected const string BoostPropertyName = "$boost";

    protected JintCoraxDocumentConverterBase(Index index, bool storeValue, bool indexImplicitNull, bool indexEmptyEntries, int numberOfBaseFields, string keyFieldName,
        string storeValueFieldName, ICollection<IndexField> fields = null) : base(index, storeValue, indexImplicitNull, indexEmptyEntries, numberOfBaseFields,
        keyFieldName, storeValueFieldName, fields)
    {
    }

    protected static bool TryGetBoostedValue(ObjectInstance valueToCheck, out JsValue value, out float? boost)
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

        return true;
    }

    protected static JsValue TryDetectDynamicFieldCreation(string property, ObjectInstance valueAsObject, IndexField field)
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

    public override void Dispose()
    {
        base.Dispose();
    }
}
