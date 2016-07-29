using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using Raven.Client.Linq;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public class DynamicBlittableJson : DynamicObject, IEnumerable<object>
    {
        protected BlittableJsonReaderObject BlittableJsonReaderObject;

        private static readonly DynamicNullObject Null = new DynamicNullObject { IsExplicitNull = true };

        public DynamicBlittableJson(BlittableJsonReaderObject blittableJsonReaderObject)
        {
            BlittableJsonReaderObject = blittableJsonReaderObject;
        }

        public void Set(BlittableJsonReaderObject blittableJsonReaderObject)
        {
            BlittableJsonReaderObject = blittableJsonReaderObject;
        }

        public string[] GetPropertyNames()
        {
            return BlittableJsonReaderObject.GetPropertyNames();
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            return TryGetByName(binder.Name, out result);
        }

        public bool TryGetByName(string name, out object result)
        {
            if (BlittableJsonReaderObject.TryGetMember(name, out result) == false)
            {
                result = Null;
                return true;
            }

            result = TransformValue(result);
            return true;
        }

        public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
        {
            return TryGetByName((string)indexes[0], out result);
        }

        public IEnumerator<object> GetEnumerator()
        {
            foreach (var propertyName in BlittableJsonReaderObject.GetPropertyNames())
            {
                yield return new KeyValuePair<object, object>(propertyName, TransformValue(BlittableJsonReaderObject[propertyName]));
            }
        }

        public override string ToString()
        {
            return BlittableJsonReaderObject.ToString();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private static object TransformValue(object value)
        {
            if (value == null)
                return Null;

            var jsonObject = value as BlittableJsonReaderObject;
            if (jsonObject != null)
                return new DynamicBlittableJson(jsonObject);

            var jsonArray = value as BlittableJsonReaderArray;
            if (jsonArray != null)
                return new DynamicArray(jsonArray);

            return value;
        }
    }
}