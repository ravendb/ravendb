using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Patch;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public sealed class JintLuceneDocumentConverter : JintLuceneDocumentConverterBase
    {
        public JintLuceneDocumentConverter(MapIndex index, bool storeValue = false)
            : base(index, index.Definition.IndexDefinition, storeValue: storeValue)
        {
        }

        public JintLuceneDocumentConverter(MapReduceIndex index, bool storeValue = false)
            : base(index, index.Definition.IndexDefinition, storeValue: storeValue)
        {
        }
    }

    public abstract class JintLuceneDocumentConverterBase : LuceneDocumentConverterBase
    {
        private readonly IndexFieldOptions _allFields;

        protected JintLuceneDocumentConverterBase(Index index, IndexDefinition indexDefinition, int numberOfBaseFields = 1, string keyFieldName = null, bool storeValue = false, string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
            : base(index, index.Configuration.IndexEmptyEntries, numberOfBaseFields, keyFieldName, storeValue, storeValueFieldName)
        {
            indexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out _allFields);
        }

        private const string ValuePropertyName = "$value";
        private const string OptionsPropertyName = "$options";
        private const string NamePropertyName = "$name";
        private const string SpatialPropertyName = "$spatial";
        private const string BoostPropertyName = "$boost";

        protected override int GetFields<T>(T instance, LazyStringValue key, LazyStringValue sourceDocumentId, object document, JsonOperationContext indexContext, IWriteOperationBuffer writeBuffer)
        {
            if (!(document is ObjectInstance documentToProcess))
                return 0;

            int newFields = 0;
            if (key != null)
            {
                instance.Add(GetOrCreateKeyField(key));
                newFields++;
            }

            if (sourceDocumentId != null)
            {
                instance.Add(GetOrCreateSourceDocumentIdField(sourceDocumentId));
                newFields++;
            }

            if (_storeValue)
            {
                var storedValue = JsBlittableBridge.Translate(indexContext,
                    documentToProcess.Engine,
                    documentToProcess);

                instance.Add(GetStoredValueField(storedValue, writeBuffer));
                newFields++;
            }

            if (TryGetBoostedValue(documentToProcess, out var boostedValue, out var documentBoost))
            {
                if (IsObject(boostedValue) == false)
                    throw new InvalidOperationException($"Invalid boosted value. Expected object but got '{boostedValue.Type}' with value '{boostedValue}'.");

                documentToProcess = boostedValue.AsObject();
            }

            foreach (var (property, propertyDescriptor) in documentToProcess.GetOwnProperties())
            {
                var propertyAsString = property.AsString();

                if (_fields.TryGetValue(propertyAsString, out var field) == false)
                    field = _fields[propertyAsString] = IndexField.Create(propertyAsString, new IndexFieldOptions(), _allFields);

                object value;
                float? propertyBoost = null;
                int numberOfCreatedFields;
                var actualValue = propertyDescriptor.Value;
                var isObject = IsObject(actualValue);
                if (isObject)
                {
                    if (TryGetBoostedValue(actualValue.AsObject(), out boostedValue, out propertyBoost))
                    {
                        actualValue = boostedValue;
                        isObject = IsObject(actualValue);
                    }

                    if (isObject)
                    {
                        //In case TryDetectDynamicFieldCreation finds a dynamic field it will populate 'field.Name' with the actual property name
                        //so we must use field.Name and not property from this point on.
                        var val = TryDetectDynamicFieldCreation(propertyAsString, actualValue.AsObject(), field);
                        if (val != null)
                        {
                            if (val.IsObject() && val.AsObject().TryGetValue(SpatialPropertyName, out _))
                            {
                                actualValue = val; //Here we populate the dynamic spatial field that will be handled below.
                            }
                            else
                            {
                                value = TypeConverter.ToBlittableSupportedType(val, flattenArrays: false, forIndexing: true, engine: documentToProcess.Engine, context: indexContext);
                                numberOfCreatedFields = GetRegularFields(instance, field, CreateValueForIndexing(value, propertyBoost), indexContext, out _);

                                newFields += numberOfCreatedFields;

                                BoostDocument(instance, numberOfCreatedFields, documentBoost);

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
                            SpatialField spatialField;
                            IEnumerable<AbstractField> spatial;
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
                                    continue; //Ignoring bad spatial field
                                }
                            }
                            else
                            {
                                continue; //Ignoring bad spatial field
                            }

                            numberOfCreatedFields = GetRegularFields(instance, field, CreateValueForIndexing(spatial, propertyBoost), indexContext, out _);

                            newFields += numberOfCreatedFields;

                            BoostDocument(instance, numberOfCreatedFields, documentBoost);

                            continue;
                        }
                    }
                }

                value = TypeConverter.ToBlittableSupportedType(actualValue, flattenArrays: false, forIndexing: true, engine: documentToProcess.Engine, context: indexContext);
                numberOfCreatedFields = GetRegularFields(instance, field, CreateValueForIndexing(value, propertyBoost), indexContext, out _);

                newFields += numberOfCreatedFields;

                BoostDocument(instance, numberOfCreatedFields, documentBoost);

                if (value is IDisposable toDispose)
                {
                    // the value was converted to a lucene field and isn't needed anymore
                    toDispose.Dispose();
                }
            }

            return newFields;

            static bool IsObject(JsValue value)
            {
                return value.IsObject() && value.IsArray() == false;
            }

            static object CreateValueForIndexing(object value, float? boost)
            {
                if (boost.HasValue == false)
                    return value;

                return new BoostedValue
                {
                    Boost = boost.Value,
                    Value = value
                };
            }

            static void BoostDocument(T instance, int numberOfCreatedFields, float? boost)
            {
                if (boost.HasValue == false)
                    return;

                var fields = instance.GetFields();
                for (int idx = fields.Count - 1; numberOfCreatedFields > 0; numberOfCreatedFields--, idx--)
                {
                    var luceneField = fields[idx];
                    luceneField.Boost = boost.Value;
                    luceneField.OmitNorms = false;
                }
            }
        }

        private static bool TryGetBoostedValue(ObjectInstance valueToCheck, out JsValue value, out float? boost)
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

        private static JsValue TryDetectDynamicFieldCreation(string property, ObjectInstance valueAsObject, IndexField field)
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
                    throw new ArgumentException($"Could not parse dynamic field option property '{propertyName}' value ('{optionValueAsString}') into '{typeof(TEnum).Name}' enum.");

                return (TEnum)enumValue;
            }
        }
    }
}
