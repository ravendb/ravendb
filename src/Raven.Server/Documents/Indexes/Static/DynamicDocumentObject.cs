using System;
using System.Dynamic;
using System.Globalization;
using System.Reflection;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Json.Linq;
using Raven.Server.Json;
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

            return Convert<T>(result, false);
        }

        internal static U Convert<U>(object value, bool cast)
        {
            if (cast)
            {
                // HACK
                return (U)value;
            }

            if (value == null)
                return default(U);

            if (value is U)
                return (U)value;

            Type targetType = typeof(U);

            if (targetType.GetTypeInfo().IsGenericType && targetType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                targetType = Nullable.GetUnderlyingType(targetType);
            }

            if (targetType == typeof(Guid))
            {
                return (U)(object)new Guid(value.ToString());
            }

            if (targetType == typeof(string))
            {
                return (U)(object)value.ToString();
            }

            if (targetType == typeof(DateTime))
            {
                var s = value as string ?? value as LazyStringValue;

                if (s != null)
                {
                    DateTime dateTime;
                    if (DateTime.TryParseExact(s, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                        DateTimeStyles.RoundtripKind, out dateTime))
                        return (U)(object)dateTime;

                    dateTime = RavenJsonTextReader.ParseDateMicrosoft(s);
                    return (U)(object)dateTime;
                }
            }

            if (targetType == typeof(DateTimeOffset))
            {
                var s = value as string ?? value as LazyStringValue;

                if (s != null)
                {
                    DateTimeOffset dateTimeOffset;
                    if (DateTimeOffset.TryParseExact(s, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                        DateTimeStyles.RoundtripKind, out dateTimeOffset))
                        return (U)(object)dateTimeOffset;

                    return default(U);
                }
            }

            try
            {
                return (U)System.Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException(string.Format("Unable to find suitable conversion for {0} since it is not predefined ", value), e);
            }
        }

        public static implicit operator BlittableJsonReaderObject(DynamicDocumentObject self)
        {
            return self._document.Data;
        }
    }
}