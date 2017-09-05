using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Json;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Lucene.Net.Store;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Collation;
using Raven.Server.Documents.Queries.Results;
using Sparrow.Json;


namespace Raven.Server.Documents.Patch
{
    public class BlittableObjectInstance : ObjectInstance
    {
        public bool Changed;
        private readonly BlittableObjectInstance _parent;
        public readonly DateTime? LastModified;
        public readonly BlittableJsonReaderObject Blittable;
        public readonly string DocumentId;
        public HashSet<string> Deletes;
        public Dictionary<string, BlittableObjectProperty> OwnValues = new Dictionary<string, BlittableObjectProperty>();
        public Dictionary<string, BlittableJsonToken> OriginalPropertiesTypes;
        public Lucene.Net.Documents.Document LuceneDocument;
        public IState LuceneState;

        private void MarkChanged()
        {
            Changed = true;
            _parent?.MarkChanged();
        }

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
                Deletes?.Remove(key);
            }
            return value.Value.AsObject();
        }

        public sealed class BlittableObjectProperty : PropertyDescriptor
        {
            private readonly BlittableObjectInstance _parent;
            private readonly string _property;
            private JsValue _value;
            public bool Changed;

            public override string ToString()
            {
                return _property;
            }

            public BlittableObjectProperty(BlittableObjectInstance parent, string property)
                : base(null, true, null, null)
            {
                _parent = parent;
                _property = property;
                var index = _parent.Blittable?.GetPropertyIndex(_property);
                if (index == null || index == -1)
                {
                    if (_parent.LuceneDocument != null)
                    {
                        // if it isn't on the document, check if it is stored in the index?
                        var fieldType = QueryResultRetrieverBase.GetFieldType(property, _parent.LuceneDocument);
                        var values = _parent.LuceneDocument.GetValues(property, _parent.LuceneState);
                        if (fieldType.IsArray)
                        {
                            Value = JsValue.FromObject(_parent.Engine, values);
                        }
                        else if (values.Length == 1)
                        {
                            Value = fieldType.IsJson
                                ? new JsonParser(_parent.Engine).Parse(values[0])
                                : new JsValue(values[0]);
                        }
                        else
                        {
                            Value = JsValue.Undefined;
                        }
                    }
                    else
                    {
                        Value = JsValue.Undefined;
                    }
                }
                else
                {
                    Value = GetPropertyValue(_property, index.Value);
                }
            }

            public override JsValue Value
            {
                get => _value;
                set
                {
                    if (Equals(value, _value))
                        return;
                    _value = value;
                    _parent.MarkChanged();
                    Changed = true;
                }
            }

            private JsValue GetPropertyValue(string key, int propertyIndex)
            {
                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

                _parent.Blittable.GetPropertyByIndex(propertyIndex, ref propertyDetails, true);

                return TranslateToJs(_parent, key, propertyDetails.Token, propertyDetails.Value);
            }

            private JsValue TranslateToJs(BlittableObjectInstance owner, string key, BlittableJsonToken type, object value)
            {
                switch (type & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.Null:
                        return JsValue.Null;
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
                        Changed = true;
                        _parent.MarkChanged();
                        return new JsValue(new BlittableObjectInstance(owner.Engine,
                            owner,
                            (BlittableJsonReaderObject)value, null, null));
                    case BlittableJsonToken.StartArray:
                        Changed = true;
                        _parent.MarkChanged();
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

        public BlittableObjectInstance(Engine engine,
            BlittableObjectInstance parent,
            BlittableJsonReaderObject blittable,
            string docId,
            DateTime? lastModified) : base(engine)
        {
            _parent = parent;
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
            Deletes?.Remove(propertyName);
            OwnValues[propertyName] = val = new BlittableObjectProperty(this, propertyName);
            return val;
        }

        public override IEnumerable<KeyValuePair<string, PropertyDescriptor>> GetOwnProperties()
        {
            foreach (var value in OwnValues)
            {
                yield return new KeyValuePair<string, PropertyDescriptor>(value.Key, value.Value);
            }
            if (Blittable == null)
                yield break;
            foreach (var prop in Blittable.GetPropertyNames())
            {
                if (Deletes?.Contains(prop) == true)
                    continue;
                if (OwnValues.ContainsKey(prop))
                    continue;
                yield return new KeyValuePair<string, PropertyDescriptor>(
                    prop,
                    GetOwnProperty(prop)
                    );
            }
        }

        private void RecordNumericFieldType(string key, BlittableJsonToken type)
        {
            if (OriginalPropertiesTypes == null)
                OriginalPropertiesTypes = new Dictionary<string, BlittableJsonToken>();
            OriginalPropertiesTypes[key] = type;
        }
    }
}
