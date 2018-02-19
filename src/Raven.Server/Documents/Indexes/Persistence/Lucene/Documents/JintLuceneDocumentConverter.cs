using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Raven.Client.Documents.Indexes;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public class JintLuceneDocumentConverter: LuceneDocumentConverterBase
    {
        public JintLuceneDocumentConverter(ICollection<IndexField> fields, bool reduceOutput = false) : base(fields, reduceOutput)
        {
        }

        private static readonly IndexFieldOptions DefaultFieldOptionds = new IndexFieldOptions();

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

            foreach ((var property, var propertyDescriptor) in documentToProcess.GetOwnProperties())
            {
                if (_fields.TryGetValue(property, out var field) == false)
                {
                    field = IndexField.Create(property, DefaultFieldOptionds, DefaultFieldOptionds);
                    _fields.Add(property, field);
                }

                foreach (var value in GetValue(propertyDescriptor.Value))
                {
                    newFields += GetRegularFields(instance, field, value, indexContext);
                }                
            }

            return newFields;
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
                if(_oneItemArray == null)
                    _oneItemArray = new JsValue[1];
                _oneItemArray[0] = jsValue;
                try
                {
                    // json string of the object
                    yield return jsValue.AsObject().Engine.Json.Stringify(JsValue.Null, _oneItemArray);
                }
                finally
                {
                    _oneItemArray[0] = null;
                }
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
    }
}
