using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
using Sparrow.Json;
using TypeConverter = Raven.Server.Utils.TypeConverter;
using JavaScriptFieldName = Raven.Client.Constants.Documents.Indexing.Fields.JavaScript;

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

        private readonly bool _ticksSupport;
        private readonly bool _dynamicFieldsDynamicAnalyzer;

        protected JintLuceneDocumentConverterBase(Index index, IndexDefinition indexDefinition, int numberOfBaseFields = 1, string keyFieldName = null,
            bool storeValue = false, string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
            : base(index, index.Configuration.IndexEmptyEntries, numberOfBaseFields, keyFieldName, storeValue, storeValueFieldName)
        {
            indexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out _allFields);

            Debug.Assert(index.Type.IsJavaScript());

            _ticksSupport = IndexDefinitionBaseServerSide.IndexVersion.IsTimeTicksInJavaScriptIndexesSupported(index.Definition.Version);
            _dynamicFieldsDynamicAnalyzer = index.Definition.Version >= IndexDefinitionBaseServerSide.IndexVersion.JavaScriptProperlyHandleDynamicFieldsIndexFields;
        }
        
        protected override int GetFields<T>(T instance, LazyStringValue key, LazyStringValue sourceDocumentId, object document, JsonOperationContext indexContext,
            IWriteOperationBuffer writeBuffer, object sourceDocument)
        {
            if (!(document is ObjectInstance documentToProcess))
                return 0;

            var currentIndexingScope = CurrentIndexingScope.Current;
            
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

                Document.Boost = documentBoost.Value;
            }
            else
            {
                Document.Boost = LuceneDefaultBoost;
            }

            foreach (var (property, propertyDescriptor) in documentToProcess.GetOwnProperties())
            {
                var propertyAsString = property.AsString();

                if (_fields.TryGetValue(propertyAsString, out var field) == false)
                    field = _fields[propertyAsString] = IndexField.Create(propertyAsString, new IndexFieldOptions(), _allFields);
                var isDynamicFieldEnumerable = IsDynamicFieldEnumerable(propertyDescriptor.Value, propertyAsString, ref field, out var iterator);
                
                bool shouldSaveAsBlittable;
                object value;
                float? propertyBoost;
                int numberOfCreatedFields = 0;
                JsValue actualValue;

                if (isDynamicFieldEnumerable)
                {
                    do
                    {
                        ProcessObject(iterator.Current, propertyAsString, ref field, isDynamicFieldsEnumeratorScope: true, out shouldSaveAsBlittable, out value, out propertyBoost, out var innerNumberOfCreatedFields,
                            out actualValue);
                        numberOfCreatedFields += innerNumberOfCreatedFields;

                        if (shouldSaveAsBlittable)
                            numberOfCreatedFields += ProcessAsJson(actualValue, field, propertyBoost);
                        
                        if (value is IDisposable toDispose)
                        {
                            // the value was converted to a lucene field and isn't needed anymore
                            toDispose.Dispose();
                        }
                    } while (iterator.MoveNext());
                }
                else
                {
                    ProcessObject(propertyDescriptor.Value, propertyAsString, ref field,  isDynamicFieldsEnumeratorScope: false, out shouldSaveAsBlittable, out value, out propertyBoost, out numberOfCreatedFields, out actualValue);
                    if (shouldSaveAsBlittable)
                        numberOfCreatedFields += ProcessAsJson(actualValue, field, propertyBoost);
                    if (value is IDisposable toDispose)
                    {
                        // the value was converted to a lucene field and isn't needed anymore
                        toDispose.Dispose();
                    }
                }
                newFields += numberOfCreatedFields;
                
                BoostDocument(instance, numberOfCreatedFields, documentBoost);
            }

            return newFields;

            void ProcessObject(JsValue valueToInsert, in string propertyAsString, ref IndexField field, bool isDynamicFieldsEnumeratorScope, out bool shouldProcessAsBlittable,
                out object value, out float? propertyBoost, out int numberOfCreatedFields, out JsValue actualValue)
            {
                value = null;
                propertyBoost = null;
                numberOfCreatedFields = 0;
                actualValue = valueToInsert;
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
                        //In case TryDetectDynamicFieldCreation finds a dynamic field it will replace `field` reference to the new IndexField object.
                        var val = TryDetectDynamicFieldCreation(propertyAsString, actualValue.AsObject(), ref field);
                        if (val != null)
                        {
                            if (val.IsObject() && val.AsObject().TryGetValue(JavaScriptFieldName.SpatialPropertyName, out _))
                            {
                                actualValue = val; //Here we populate the dynamic spatial field that will be handled below.
                            }
                            else
                            {
                                value = TypeConverter.ToBlittableSupportedType(val, flattenArrays: false, forIndexing: true, canTryJsStringToDateConversion: _ticksSupport, engine: documentToProcess.Engine,
                                    context: indexContext);

                                //We've to wrap dynamic fields to avoid fields cache (and value override), solves: RavenDB_15983
                                //Also backward compatibility for older indexes.
                                numberOfCreatedFields = isDynamicFieldsEnumeratorScope && _dynamicFieldsDynamicAnalyzer
                                    ? GetRegularFields(instance, field, LuceneCreateField(currentIndexingScope, field, CreateValueForIndexing(value, propertyBoost)), indexContext, sourceDocument, out _) 
                                    : GetRegularFields(instance, field, CreateValueForIndexing(value, propertyBoost), indexContext, sourceDocument, out _);

                                newFields += numberOfCreatedFields;

                                BoostDocument(instance, numberOfCreatedFields, documentBoost);

                                if (value is IDisposable toDispose1)
                                {
                                    // the value was converted to a lucene field and isn't needed anymore
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
                            IEnumerable<AbstractField> spatial;
                            if (inner.IsString())
                            {
                                spatialField = AbstractStaticIndexBase.GetOrCreateSpatialField(field.Name);
                                spatial = (IEnumerable<AbstractField>)AbstractStaticIndexBase.CreateSpatialField(spatialField, inner.AsString());
                            }
                            else if (inner.IsObject())
                            {
                                var innerObject = inner.AsObject();
                                if (innerObject.HasOwnProperty("Lat") && innerObject.HasOwnProperty("Lng") && innerObject.TryGetValue("Lat", out var lat)
                                    && lat.IsNumber() && innerObject.TryGetValue("Lng", out var lng) && lng.IsNumber())
                                {
                                    spatialField = AbstractStaticIndexBase.GetOrCreateSpatialField(field.Name);
                                    spatial = (IEnumerable<AbstractField>)AbstractStaticIndexBase.CreateSpatialField(spatialField, lat.AsNumber(), lng.AsNumber());
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

                            numberOfCreatedFields = GetRegularFields(instance, field, CreateValueForIndexing(spatial, propertyBoost), indexContext, sourceDocument, out _);

                            newFields += numberOfCreatedFields;

                            BoostDocument(instance, numberOfCreatedFields, documentBoost);

                            shouldProcessAsBlittable = false;
                            return;
                        }
                    }
                }

                shouldProcessAsBlittable = true;
            }

            int ProcessAsJson(JsValue actualValue, IndexField field, float? propertyBoost)
            {
                var value = TypeConverter.ToBlittableSupportedType(actualValue, flattenArrays: false, forIndexing: true, canTryJsStringToDateConversion: _ticksSupport, engine: documentToProcess.Engine,
                    context: indexContext);
                return GetRegularFields(instance, field, CreateValueForIndexing(value, propertyBoost), indexContext, sourceDocument, out _);
            }

            bool IsDynamicFieldEnumerable(JsValue propertyDescriptorValue, string propertyAsString, ref IndexField field, out IEnumerator<JsValue> iterator)
            {
                iterator = Enumerable.Empty<JsValue>().GetEnumerator();

                if (propertyDescriptorValue.IsArray() == false)
                    return false;

                iterator = propertyDescriptorValue.AsArray().GetEnumerator();
                if (iterator.MoveNext() == false || iterator.Current is null || iterator.Current.IsObject() == false || iterator.Current.IsArray() == true)
                    return false;

                var valueAsObject = iterator.Current.AsObject();

                return TryDetectDynamicFieldCreation(propertyAsString, valueAsObject, ref field) is not null
                       || valueAsObject.HasOwnProperty(JavaScriptFieldName.SpatialPropertyName);
                }

            static bool IsObject(JsValue value)
            {
                return value.IsObject() && value.IsArray() == false;
            }

            static object CreateValueForIndexing(object value, float? boost)
            {
                if (boost.HasValue == false)
                    return value;

                return new BoostedValue {Boost = boost.Value, Value = value};
            }

            static void BoostDocument(T instance, int numberOfCreatedFields, float? boost)
            {
                if (boost.HasValue == false)
                    return;

                var fields = instance.GetFields();
                for (int idx = fields.Count - 1; numberOfCreatedFields > 0; numberOfCreatedFields--, idx--)
                {
                    var luceneField = fields[idx];
                    luceneField.OmitNorms = false;
                }
            }
        }

        private static bool TryGetBoostedValue(ObjectInstance valueToCheck, out JsValue value, out float? boost)
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

        private JsValue TryDetectDynamicFieldCreation(string property, ObjectInstance valueAsObject, ref IndexField field)
        {
            //We have a field creation here _ = {"$value":val, "$name","$options":{...}}
            if (!valueAsObject.HasOwnProperty(JavaScriptFieldName.ValuePropertyName))
                return null;

            var value = valueAsObject.GetOwnProperty(JavaScriptFieldName.ValuePropertyName).Value;
            PropertyDescriptor nameProperty = valueAsObject.GetOwnProperty(JavaScriptFieldName.NamePropertyName);

            string fieldName;
            FieldIndexing? fieldIndexing = null;
            FieldStorage? fieldStorage = null;
            FieldTermVector? termVector = null;
            
            if (nameProperty != null)
            {
                var fieldNameObj = nameProperty.Value;
                if (fieldNameObj.IsString() == false)
                    throw new ArgumentException($"Dynamic field {property} is expected to have a string {JavaScriptFieldName.NamePropertyName} property but got {fieldNameObj}");

                fieldName = fieldNameObj.AsString();
            }
            else
            {
                fieldName = property;
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
                        fieldIndexing = GetEnum<FieldIndexing>(optionValue, propertyNameAsString);

                        continue;
                    }

                    if (string.Equals(propertyNameAsString, nameof(CreateFieldOptions.Storage), StringComparison.OrdinalIgnoreCase))
                    {
                        if (optionValue.IsBoolean())
                            fieldStorage = optionValue.AsBoolean()
                                ? FieldStorage.Yes
                                : FieldStorage.No;
                        else
                            fieldStorage = GetEnum<FieldStorage>(optionValue, propertyNameAsString);

                        continue;
                    }

                    if (string.Equals(propertyNameAsString, nameof(CreateFieldOptions.TermVector), StringComparison.OrdinalIgnoreCase))
                    {
                        termVector = GetEnum<FieldTermVector>(optionValue, propertyNameAsString);

                        continue;
                    }
                }
            }

            field = IndexField.Create(fieldName, 
                new IndexFieldOptions()
                {
                    Indexing = fieldIndexing, 
                    Storage = fieldStorage, 
                    TermVector = termVector
                }, _allFields);

            if (_dynamicFieldsDynamicAnalyzer)
            {
                //This will get analyzer from dynamic field configuration.
                CurrentIndexingScope.Current.DynamicFields ??= new();
                CurrentIndexingScope.Current.DynamicFields[fieldName] = field;
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
        
        private static IEnumerable<AbstractField> LuceneCreateField(CurrentIndexingScope scope, IndexField field, object value)
        {
            scope.DynamicFields ??= new Dictionary<string, IndexField>();
            scope.DynamicFields[field.Name] = field;
            scope.CreateFieldConverter ??= new LuceneDocumentConverter(scope.Index, new IndexField[] { });

            using var i = scope.CreateFieldConverter.NestedField(scope.CreatedFieldsCount);
            scope.IncrementDynamicFields();
            var result = new List<AbstractField>();
            scope.CreateFieldConverter.GetRegularFields(new AbstractStaticIndexBase.StaticIndexLuceneDocumentWrapper(result), field, value, CurrentIndexingScope.Current.IndexContext, scope?.Source, out _);
            return result;
        }
    }
}
