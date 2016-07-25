using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Reflection;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Json.Linq;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public class DynamicDocumentObject : DynamicObject
    {
        private readonly DynamicBlittableJson _dynamicJson;
        private Document _document;

        public DynamicDocumentObject(Document document)
        {
            _document = document;
            _dynamicJson = new DynamicBlittableJson(document.Data);
        }

        public DynamicDocumentObject()
        {
            _dynamicJson = new DynamicBlittableJson(null);
        }

        private DynamicDocumentObject(DynamicBlittableJson dynamicJson)
        {
            _dynamicJson = dynamicJson;
        }

        public void Set(Document document)
        {
            _document = document;
            _dynamicJson.Set(_document.Data);
        }

        public object this[string key]
        {
            get
            {
                if (Constants.Headers.LastModified.Equals(key, StringComparison.OrdinalIgnoreCase))
                    key = Constants.Headers.RavenLastModified;

                object result;
                if (_dynamicJson.TryGetByName(key, out result) == false)
                    throw new InvalidOperationException($"Could not get '{key}' value of dynamic object");

                if (Constants.Metadata.Equals(key, StringComparison.Ordinal))
                    return new DynamicDocumentObject((DynamicBlittableJson)result);

                return result;
            }
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            var name = binder.Name;

            if (name == Constants.DocumentIdFieldName)
            {
                result = _document.Key;
                return true;
            }

            if (name == "Id")
            {
                result = _document.Key;
                return true;
            }

            var getResult = _dynamicJson.TryGetMember(binder, out result);

            if (name == "HasValue" && result == null)
            {
                result = false;
                return true;
            }

            if (result is LazyDoubleValue)
            {
                double doubleResult;
                long longResult;

                switch (BlittableNumber.Parse(result, out doubleResult, out longResult))
                {
                    case NumberParseResult.Double:
                        result = doubleResult;
                        break;
                    case NumberParseResult.Long:
                        result = longResult;
                        break;
                }
            }

            //if (result is LazyStringValue)
            //{
            //    TODO arek - this is necessary only to handle methods of string - e.g. .Substring
            //    we should be able to recognize that base on definition of static index
            //    result = result.ToString(); 
            //}

            return getResult;
        }

        public T Value<T>(string key)
        {
            if (Constants.Headers.LastModified.Equals(key, StringComparison.OrdinalIgnoreCase))
                key = Constants.Headers.RavenLastModified; // TODO arek - need to handle it better

            object result;

            if (_dynamicJson.TryGetByName(key, out result) == false)
                throw new InvalidOperationException($"Could not get '{key}' value of dynamic object");

            return TypeConverter.Convert<T>(result, false);
        }

        public IEnumerable<object> Select(Func<object, object> func)
        {
            var list = new List<object>();
            foreach (var property in _dynamicJson.GetPropertyNames())
            {
                object result;
                if (_dynamicJson.TryGetByName(property, out result) == false)
                    throw new InvalidOperationException("Should not happen.");

                list.Add(func(new KeyValuePair<string, object>(property, result)));
            }

            return new DynamicBlittableJson.DynamicArray(list);
        }

        public static implicit operator BlittableJsonReaderObject(DynamicDocumentObject self)
        {
            return self._document.Data;
        }
    }
}