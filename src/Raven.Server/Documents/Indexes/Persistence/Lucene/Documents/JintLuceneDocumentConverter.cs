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

        private static IndexFieldOptions _defaultFieldOptionds = new IndexFieldOptions();

        protected override int GetFields<T>(T instance, LazyStringValue key, object document, JsonOperationContext indexContext)
        {
            var documentToProcess = document as ObjectInstance;
            if (documentToProcess == null)
                return 0;

            int newFields = 0;
            if (key != null)
            {
                instance.Add(GetOrCreateKeyField(key));
                newFields++;
            }

            foreach ((var property, var propertyDescriptor) in documentToProcess.GetOwnProperties())
            {
                IndexField field;

                if (_fields.TryGetValue(property, out field) == false)
                {
                    field = IndexField.Create(property, _defaultFieldOptionds, _defaultFieldOptionds);
                    _fields.Add(property, field);
                }

                foreach (var value in GetValue(propertyDescriptor.Value))
                {
                    newFields += GetRegularFields(instance, field, value, indexContext);
                }                
            }

            return newFields;
        }

        private IEnumerable GetValue(JsValue jsValue)
        {
            if (jsValue.IsNull())
                yield return null;
            if (jsValue.IsString())
                yield return jsValue.AsString();
            if (jsValue.IsBoolean())
                yield return jsValue.AsBoolean();
            if (jsValue.IsNumber())
                yield return jsValue.AsNumber();
            if (jsValue.IsDate())
                yield return jsValue.AsDate();
            if (jsValue.IsObject())
                yield return jsValue.ToString();                        
            if (jsValue.IsArray())
            {
                var arr = jsValue.AsArray();
                var len = arr.GetLength();
                for (var i = 0; i < len; i++)
                {
                    foreach (var val in GetValue(arr.Get(i.ToString())))
                    {
                        yield return val;
                    }
                }
            }
        }
    }
}
