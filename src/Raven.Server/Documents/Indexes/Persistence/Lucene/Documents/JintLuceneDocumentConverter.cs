using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Json.Converters;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public class JintLuceneDocumentConverter: LuceneDocumentConverterBase
    {
        public JintLuceneDocumentConverter(ICollection<IndexField> fields, bool reduceOutput = false) : base(fields, reduceOutput)
        {
        }

        private static readonly IndexFieldOptions DefaultFieldOptionds = new IndexFieldOptions();

        private readonly string CreatedFieldValuePropertyName = "$value";
        private readonly string CreatedFieldOptionsPropertyName = "$options";
        private readonly string CreatedFieldNamePropertyName = "$name";


        protected override int GetFields<T>(T instance, LazyStringValue key, object document, JsonOperationContext indexContext)
        {
            if (!(document is ObjectInstance documentToProcess))
                return 0;

            int newFields = 0;
            if (key != null)
            {
                instance.Add(GetOrCreateKeyField(key));
                newFields++;
            }

            IndexField allFields = null;
            _fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out allFields);
            var AllFieldsOptions = allFields?.ToIndexFieldOptions();
            foreach ((var property, var propertyDescriptor) in documentToProcess.GetOwnProperties())
            {
                var actualValue = propertyDescriptor.Value;
                var actualFieldOption = DefaultFieldOptionds;
                string actualFieldName = property;
                if (_fields.TryGetValue(property, out var field) == false)
                {
                    var shouldCreateField = true;
                    if (actualValue.IsObject())
                    {
                        if (TryDetectDynamicFieldCreation(ref actualValue, property, ref actualFieldOption, out actualFieldName))
                        {
                            shouldCreateField = _fields.ContainsKey(actualFieldName) == false;
                        }
                    }

                    if (shouldCreateField && actualFieldName != JavaScriptIndex.DynamicFieldName)
                    {
                        field = IndexField.Create(actualFieldName, actualFieldOption, AllFieldsOptions);

                        _fields.Add(actualFieldName, field);
                    }                    
                }

                
                

                foreach (var value in GetValue(actualValue))
                {
                    newFields += GetRegularFields(instance, field, value, indexContext);
                }                
            }

            return newFields;
        }

        private static readonly string[] IndexFieldValues = {"index", "Index"};

        private static readonly string[] StoreFieldValues = { "store", "Store" };

        private bool TryDetectDynamicFieldCreation(ref JsValue actualValue, string property, ref IndexFieldOptions actualFieldOption, out string fieldName)
        {
            fieldName = null;
            var valueAsObject = actualValue.AsObject();
            //We have a field creation here _ = {"$value":val, "$name","$options":{...}}
            if (valueAsObject.HasOwnProperty(CreatedFieldValuePropertyName)
            && valueAsObject.HasOwnProperty(CreatedFieldNamePropertyName))
            {
                actualValue = valueAsObject.GetOwnProperty(CreatedFieldValuePropertyName).Value;
                var fieldNameObj = valueAsObject.GetOwnProperty(CreatedFieldNamePropertyName).Value;
                if(fieldNameObj.IsString() == false)
                    throw new ArgumentException($"Dynamic field {property} is expected to have a string {CreatedFieldNamePropertyName} property but got {fieldNameObj}");
                fieldName = fieldNameObj.AsString();
                if (valueAsObject.HasOwnProperty(CreatedFieldOptionsPropertyName))
                {
                    var options = valueAsObject.GetOwnProperty(CreatedFieldOptionsPropertyName).Value;
                    if (options.IsObject() == false)
                    {
                        throw new ArgumentException($"Dynamic field {property} is expected to contain an object with three properties " +
                                                    $"{CreatedFieldOptionsPropertyName}, {CreatedFieldNamePropertyName} and {CreatedFieldOptionsPropertyName} the later should be a valid IndexFieldOptions object.");
                    }
                    
                    var optionObj = options.AsObject();

                    foreach (var searchField in IndexFieldValues)
                    {
                        if (optionObj.Get(searchField).IsBoolean())
                        {
                            var indexing = optionObj.Get(searchField).AsBoolean();
                            actualFieldOption.Indexing = indexing ? FieldIndexing.Search : FieldIndexing.No;
                        }
                    }
                    foreach (var storeFieldd in StoreFieldValues)
                    {
                        if (optionObj.Get(storeFieldd).IsBoolean())
                        {
                            var store = optionObj.Get(storeFieldd).AsBoolean();
                            actualFieldOption.Storage = store ? FieldStorage.Yes : FieldStorage.No;
                        }
                    }

                    return true;
                }                
            }

            return false;
        }

        [ThreadStatic]
        private static JsValue[] _oneItemArray;

        private IEnumerable GetValue(JsValue jsValue)
        {
            if (jsValue.IsNull())
                yield return null;
            if (jsValue.IsString())
                yield return jsValue.AsString();
            if (jsValue.IsBoolean())
                yield return jsValue.AsBoolean().ToString(); // avoid boxing the boolean
            if (jsValue.IsNumber())
                yield return jsValue.AsNumber();
            if (jsValue.IsDate())
                yield return jsValue.AsDate();
            if (jsValue.IsObject())
            {
                yield return StringifyObject(jsValue);
            }                        
            if (jsValue.IsArray())
            {
                var arr = jsValue.AsArray();
                foreach ((var key, var val)  in arr.GetOwnProperties())
                {
                    if(key == "length")
                        continue;
                   
                    foreach (var innerVal in GetValue(val.Value))
                    {
                        yield return innerVal;
                    }
                }
            }
        }

        private static object StringifyObject(JsValue jsValue)
        {
            if (_oneItemArray == null)
                _oneItemArray = new JsValue[1];
            _oneItemArray[0] = jsValue;
            try
            {
                // json string of the object
                return jsValue.AsObject().Engine.Json.Stringify(JsValue.Null, _oneItemArray);
            }
            finally
            {
                _oneItemArray[0] = null;
            }
        }
    }
}
