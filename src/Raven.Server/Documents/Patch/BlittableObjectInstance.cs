using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Sparrow.Json;


namespace Raven.Server.Documents.Patch
{
    public class BlittableObjectInstance : ObjectInstance
    {
        public readonly DateTime? LastModified;
        public readonly BlittableJsonReaderObject Blittable;
        public readonly string DocumentId;
        public HashSet<string> Deletes;
        public Dictionary<string, BlittableObjectProperty> OwnValues = new Dictionary<string, BlittableObjectProperty>();
        public Dictionary<string, BlittableJsonToken> OriginalPropertiesTypes;

        public ObjectInstance GetOrCreate(string key)
        {
            BlittableObjectProperty value;
            if (OwnValues.TryGetValue(key, out value) == false)
            {
                var propertyIndex = Blittable.GetPropertyIndex(key);
                value = new BlittableObjectProperty(this, key);
                if (propertyIndex == -1)
                {
                    value.Value = new JsValue(new ObjectInstance(Engine));
                }
                OwnValues[key] = value;
            }
            return value.Value.AsObject();
        }

        public sealed class BlittableObjectProperty : PropertyDescriptor
        {
            private readonly BlittableObjectInstance _parent;
            private readonly string _property;

            public override string ToString()
            {
                return _property;
            }

            public BlittableObjectProperty(BlittableObjectInstance parent,string property)
                : base(null, true, null, null)
            {
                _parent = parent;
                _property = property;
                var index = _parent.Blittable?.GetPropertyIndex(_property);
                if (index == null || index == -1)
                {
                    Value = new JsValue(new BlittableObjectInstance(_parent.Engine, null, null, null));
                }
                else
                {
                    Value = GetPropertyValue(_property, index.Value);
                }
            }

            public override JsValue Value { get; set; }


            private JsValue GetPropertyValue(string key, int propertyIndex)
            {
                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

                _parent.Blittable.GetPropertyByIndex(propertyIndex, ref propertyDetails, true);

                return TranslateToJs(_parent, key, propertyDetails.Token, propertyDetails.Value);
            }

            private static JsValue TranslateToJs(BlittableObjectInstance owner, string key, BlittableJsonToken type, object value)
            {
                switch (type & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.Null:
                        return new JsValue(new BlittableObjectInstance(owner.Engine, null, null, null));
                    case BlittableJsonToken.Boolean:
                        return new JsValue((bool)value);
                    case BlittableJsonToken.Integer:
                        owner.RecordNumericFieldType(key, BlittableJsonToken.Integer);
                        return new JsValue((long)value);
                    case BlittableJsonToken.LazyNumber:
                        owner.RecordNumericFieldType(key, BlittableJsonToken.LazyNumber);
                        return new JsValue((double)(LazyNumberValue)value);
                    case BlittableJsonToken.String:
                        return new JsValue(((LazyStringValue)value).ToString());
                    case BlittableJsonToken.CompressedString:
                        return new JsValue(((LazyCompressedStringValue)value).ToString());
                    case BlittableJsonToken.StartObject:
                        return new JsValue(new BlittableObjectInstance(owner.Engine,
                            (BlittableJsonReaderObject)value, null, null));
                    case BlittableJsonToken.StartArray:
                        var blitArray = (BlittableJsonReaderArray)value;
                        var array = new object[blitArray.Length];
                        for (int i = 0; i < array.Length; i++)
                        {
                            var blit = blitArray.GetValueTokenTupleByIndex(i);
                            array[i] = TranslateToJs(owner, key, blit.Item2, blit.Item1);
                        }
                        return JsValue.FromObject(owner.Engine, array);
                    default:
                        throw new ArgumentOutOfRangeException(type.ToString());
                }
            }
        }

        public BlittableObjectInstance(Engine engine, BlittableJsonReaderObject blittable, string docId, DateTime? lastModified) : base(engine)
        {
            LastModified = lastModified;
            Blittable = blittable;
            DocumentId = docId;
        }

        public override bool Delete(string propertyName, bool throwOnError)
        {
            if (Deletes == null)
                Deletes = new HashSet<string>();
            Deletes.Add(propertyName);
            return OwnValues.Remove(propertyName);
        }

        public override PropertyDescriptor GetOwnProperty(string propertyName)
        {
            if (OwnValues.TryGetValue(propertyName, out var val))
                return val;
            OwnValues[propertyName] = val = new BlittableObjectProperty(this, propertyName);
            return val;
        }


        private void RecordNumericFieldType(string key, BlittableJsonToken type)
        {
            if (OriginalPropertiesTypes == null)
                OriginalPropertiesTypes = new Dictionary<string, BlittableJsonToken>();
            OriginalPropertiesTypes[key] = type;
        }
    }
}
