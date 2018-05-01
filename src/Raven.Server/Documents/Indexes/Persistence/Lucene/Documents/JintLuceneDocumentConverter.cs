using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Jint.Native;
using Jint.Native.Array;
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
    public class JintLuceneDocumentConverter : LuceneDocumentConverterBase
    {
        public JintLuceneDocumentConverter(ICollection<IndexField> fields, bool reduceOutput = false) : base(fields, reduceOutput)
        {
        }

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

            if (_reduceOutput)
            {
                var reduceResult = JsBlittableBridge.Translate(indexContext,
                    documentToProcess.Engine,
                    documentToProcess);

                instance.Add(GetReduceResultValueField(reduceResult));
                newFields++;
            }

            foreach ((var property, var propertyDescriptor) in documentToProcess.GetOwnProperties())
            {
                if (_fields.TryGetValue(property, out var field) == false)
                {
                    field = new IndexField
                    {
                        Name = property,
                        Indexing = _allFields.Indexing,
                        Storage = _allFields.Storage,
                        Analyzer = _allFields.Analyzer,
                        Spatial = _allFields.Spatial,
                        HasSuggestions = _allFields.HasSuggestions,
                        TermVector = _allFields.TermVector
                    };
                }

                var obj = propertyDescriptor.Value;
                object value;
                if (obj.IsObject() && obj.IsArray() == false)
                {
                    var val = TryDetectDynamicFieldCreation(property, obj.AsObject(), field);
                    if (val != null)
                    {
                        value = GetValue(val);
                        newFields += GetRegularFields(instance, field, value, indexContext);
                        continue;
                    }
                }

                value = GetValue(propertyDescriptor.Value);
                newFields += GetRegularFields(instance, field, value, indexContext, nestedArray: true);
            }

            return newFields;
        }

        private static readonly string[] IndexFieldValues = { "index", "Index" };

        private static readonly string[] StoreFieldValues = { "store", "Store" };

        private JsValue TryDetectDynamicFieldCreation(string property, ObjectInstance valueAsObject, IndexField field)
        {
            //We have a field creation here _ = {"$value":val, "$name","$options":{...}}
            if (!valueAsObject.HasOwnProperty(CreatedFieldValuePropertyName) ||
                !valueAsObject.HasOwnProperty(CreatedFieldNamePropertyName))
                return null;

            var value = valueAsObject.GetOwnProperty(CreatedFieldValuePropertyName).Value;
            var fieldNameObj = valueAsObject.GetOwnProperty(CreatedFieldNamePropertyName).Value;
            if (fieldNameObj.IsString() == false)
                throw new ArgumentException($"Dynamic field {property} is expected to have a string {CreatedFieldNamePropertyName} property but got {fieldNameObj}");


            field.Name = fieldNameObj.AsString();

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
                        field.Indexing = indexing ? FieldIndexing.Search : FieldIndexing.No;
                    }
                }
                foreach (var storeFieldd in StoreFieldValues)
                {
                    if (optionObj.Get(storeFieldd).IsBoolean())
                    {
                        var store = optionObj.Get(storeFieldd).AsBoolean();
                        field.Storage = store ? FieldStorage.Yes : FieldStorage.No;
                    }
                }
            }

            return value;
        }

        [ThreadStatic]
        private static JsValue[] _oneItemArray;

        private object GetValue(JsValue jsValue)
        {
            if (jsValue.IsNull())
                return null;
            if (jsValue.IsString())
                return jsValue.AsString();
            if (jsValue.IsBoolean())
                return jsValue.AsBoolean().ToString(); // avoid boxing the boolean
            if (jsValue.IsNumber())
                return jsValue.AsNumber();
            if (jsValue.IsDate())
                return jsValue.AsDate();
            //Array is an object in Jint
            if (jsValue.IsArray())
            {
                var arr = jsValue.AsArray();
                return EnumerateArray(arr);
            }
            else if (jsValue.IsObject())
            {
                return StringifyObject(jsValue);
            }

            ThrowInvalidObject(jsValue);
            return null;
        }

        private static void ThrowInvalidObject(JsValue jsValue)
        {
            throw new InvalidOperationException("Invalid type " + jsValue);
        }

        private IEnumerable EnumerateArray(ArrayInstance arr)
        {
            foreach ((var key, var val) in arr.GetOwnProperties())
            {
                if (key == "length")
                    continue;

                yield return GetValue(val.Value);
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
