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
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server;
using V8.Net;
using CoraxLib = global::Corax;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class V8CoraxDocumentConverter : V8CoraxDocumentConverterBase
{
    private readonly IndexFieldOptions _allFields;

    public V8CoraxDocumentConverter(MapIndex index, bool storeValue = false)
        : base(index, storeValue, false, true, 1, null, Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
    {
        index.Definition.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out _allFields);
    }

    public V8CoraxDocumentConverter(MapReduceIndex index, bool storeValue = false)
        : base(index, storeValue, false, true, 1, null, Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
    {
        index.Definition.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out _allFields);
    }

    //todo maciej: refactor | stop duplicating code from LuceneJint[...] https://github.com/ravendb/ravendb/pull/13730#discussion_r825928762
    //TODO: egor this is copy-paste from JsLuceneDocumentConverterV8 need to create generic JsLuceneDocumentConverter
    public override Span<byte> SetDocumentFields(LazyStringValue key, LazyStringValue sourceDocumentId, object documentObj, JsonOperationContext indexContext,
        out LazyStringValue id, Span<byte> writerBuffer)
    {
        if (!(documentObj is JsHandleV8 jsHandle))
        {
            id = null;
            return Span<byte>.Empty;
        }

        var documentToProcess = jsHandle.Item;
        if (documentToProcess.IsObject == false)
        {
            id = null;
            return Span<byte>.Empty;
        }

        var entryWriter = new CoraxLib.IndexEntryWriter(writerBuffer, GetKnownFieldsForWriter());

        id = key ?? (sourceDocumentId ?? throw new InvalidDataException("Cannot find any identifier of the document."));
        var scope = new SingleEntryWriterScope(_allocator);


        if (TryGetBoostedValue(documentToProcess, out var boostedValue, out var documentBoost))
        {
            boostedValue.Dispose();
            throw new NotSupportedException("Document boosting is not available in Corax.");
        }

        scope.Write(0, id.AsSpan(), ref entryWriter);
        int idX = 1;

        var jsPropertyValueNew = InternalHandle.Empty;
        foreach (var (property, propertyDescriptor) in documentToProcess.GetOwnProperties())
        {
            if (_fields.TryGetValue(property, out var field) == false)
            {
                field = _fields[property] = IndexField.Create(property, new IndexFieldOptions(), _allFields);
                field.Id = idX++;
            }

            object value;
            float? propertyBoost = null;

            jsPropertyValueNew.Set(propertyDescriptor);
            try
            {
                using (propertyDescriptor)
                {
                    var isObject = IsObject(jsPropertyValueNew);
                    if (isObject)
                    {
                        if (TryGetBoostedValue(jsPropertyValueNew, out boostedValue, out propertyBoost))
                        {
                            boostedValue.Dispose();
                            throw new NotSupportedException("Document field boosting is not available in Corax.");
                        }

                        if (isObject)
                        {
                            using (var val = TryDetectDynamicFieldCreation(property, jsPropertyValueNew, field))
                            {
                                if (val != null && val.IsEmpty == false)
                                {
                                    if (val.IsObject && val.HasProperty(SpatialPropertyName))
                                    {
                                        jsPropertyValueNew.Set(val); //Here we populate the dynamic spatial field that will be handled below.
                                    }
                                    else
                                    {
                                        /*value = Utils.TypeConverter.ToBlittableSupportedType(val, flattenArrays: false, forIndexing: true, engine: documentToProcess.Engine,
                                        context: indexContext);*/
                                        value = TypeConverter.ToBlittableSupportedType(val, _engineEx, flattenArrays: false, forIndexing: true, indexContext);
                                        InsertRegularField(field, value, indexContext, ref entryWriter, scope);
                                        if (value is IDisposable toDispose1)
                                        {
                                            // the value was converted to a lucene field and isn't needed anymore
                                            toDispose1.Dispose();
                                        }

                                        continue;
                                    }
                                }

                                if (jsPropertyValueNew.TryGetValue(SpatialPropertyName, out var inner))
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
                    }

                    value = TypeConverter.ToBlittableSupportedType(jsPropertyValueNew, _engineEx, flattenArrays: false, forIndexing: true, indexContext);
                }
            }
            finally
            {
                jsPropertyValueNew.Dispose();
            }

            InsertRegularField(field, value, indexContext, ref entryWriter, scope);

            if (value is IDisposable toDispose)
            {
                // the value was converted to a lucene field and isn't needed anymore
                toDispose.Dispose();
            }
        }


        if (_storeValue)
        {
            var storedValue = JsBlittableBridgeV8.Translate(indexContext, _engineEx, jsHandle);
            unsafe
            {
                using (_allocator.Allocate(storedValue.Size, out Span<byte> blittableBuffer))
                {
                    fixed (byte* bPtr = blittableBuffer)
                        storedValue.CopyTo(bPtr);

                    scope.Write(GetKnownFieldsForWriter().Count - 1, blittableBuffer, ref entryWriter);
                }
            }
        }

        entryWriter.Finish(out var output);
        return output;

        static bool IsObject(InternalHandle value)
        {
            return value.IsObject && value.IsArray == false;
        }
    }
}

public abstract class V8CoraxDocumentConverterBase : CoraxDocumentConverterBase
{
    protected const string ValuePropertyName = "$value";
    protected const string OptionsPropertyName = "$options";
    protected const string NamePropertyName = "$name";
    protected const string SpatialPropertyName = "$spatial";
    protected const string BoostPropertyName = "$boost";
    protected readonly V8EngineEx _engineEx;
    protected V8CoraxDocumentConverterBase(Index index, bool storeValue, bool indexImplicitNull, bool indexEmptyEntries, int numberOfBaseFields, string keyFieldName,
        string storeValueFieldName, ICollection<IndexField> fields = null) : base(index, storeValue, indexImplicitNull, indexEmptyEntries, numberOfBaseFields,
        keyFieldName, storeValueFieldName, fields)
    {
        var jsIndexJint = (AbstractJavaScriptIndexV8)index._compiled;
        _engineEx = jsIndexJint.EngineEx;
    }
    //TODO: egor this can be made generic for jint & v8, probably for corax & lucene as well
    protected static bool TryGetBoostedValue(InternalHandle valueToCheck, out InternalHandle value, out float? boost)
    {
        value = InternalHandle.Empty;
        boost = null;

        if (valueToCheck.TryGetValue(BoostPropertyName, out var boostValue) == false)
            return false;
        using (boostValue)
        {
            if (valueToCheck.TryGetValue(ValuePropertyName, out var valueValue) == false)
                return false;

            if (boostValue.IsNumberOrIntEx == false)
                return false;

            boost = (float)boostValue.AsDouble;
            value = valueValue;
        }

        return true;
    }

    protected static InternalHandle TryDetectDynamicFieldCreation(string property, InternalHandle valueAsObject, IndexField field)
    {
        //We have a field creation here _ = {"$value":val, "$name","$options":{...}}
        if (!valueAsObject.HasOwnProperty(ValuePropertyName))
            return InternalHandle.Empty;
        var value = valueAsObject.GetOwnProperty(ValuePropertyName);

        using (var fieldNameObj = valueAsObject.GetOwnProperty(NamePropertyName))
        {
            if (!fieldNameObj.IsUndefined)
            {
                if (fieldNameObj.IsStringEx == false)
                    throw new ArgumentException($"Dynamic field {property} is expected to have a string {NamePropertyName} property but got {fieldNameObj}");

                field.Name = fieldNameObj.AsString;
            }
            else
            {
                field.Name = property;
            }
        }

        using (var optionObj = valueAsObject.GetOwnProperty(OptionsPropertyName))
        {
            if (!optionObj.IsUndefined)
            {
                if (optionObj.IsObject == false)
                {
                    throw new ArgumentException($"Dynamic field {property} is expected to contain an object with three properties " +
                                                $"{ValuePropertyName}, {NamePropertyName} and {OptionsPropertyName} the later should be a valid IndexFieldOptions object.");
                }

                foreach (var (propertyName, jsPropertyValue) in optionObj.GetOwnProperties())
                {
                    using (jsPropertyValue)
                    {
                        if (jsPropertyValue.IsUndefined || jsPropertyValue.IsNull)
                            continue;

                        if (string.Equals(propertyName, nameof(CreateFieldOptions.Indexing), StringComparison.OrdinalIgnoreCase))
                        {
                            field.Indexing = GetEnum<FieldIndexing>(jsPropertyValue, propertyName);

                            continue;
                        }

                        if (string.Equals(propertyName, nameof(CreateFieldOptions.Storage), StringComparison.OrdinalIgnoreCase))
                        {
                            if (jsPropertyValue.IsBoolean)
                                field.Storage = jsPropertyValue.AsBoolean
                                    ? FieldStorage.Yes
                                    : FieldStorage.No;
                            else
                                field.Storage = GetEnum<FieldStorage>(jsPropertyValue, propertyName);

                            continue;
                        }

                        if (string.Equals(propertyName, nameof(CreateFieldOptions.TermVector), StringComparison.OrdinalIgnoreCase))
                        {
                            field.TermVector = GetEnum<FieldTermVector>(jsPropertyValue, propertyName);
                            continue;
                        }
                    }
                }
            }
        }

        return value;

        TEnum GetEnum<TEnum>(InternalHandle optionValue, string propertyName)
        {
            if (optionValue.IsStringEx == false)
                throw new ArgumentException($"Could not parse dynamic field option property '{propertyName}' value ('{optionValue}') because it is not a string.");

            var optionValueAsString = optionValue.AsString;
            if (Enum.TryParse(typeof(TEnum), optionValueAsString, true, out var enumValue) == false)
                throw new ArgumentException($"Could not parse dynamic field option property '{propertyName}' value ('{optionValueAsString}') into '{typeof(TEnum).Name}' enum.");

            return (TEnum)enumValue;
        }
    }

    public override void Dispose()
    {
        base.Dispose();
    }
}
