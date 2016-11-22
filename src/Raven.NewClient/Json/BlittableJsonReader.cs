using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using Newtonsoft.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Json
{
    public class BlittableJsonReader : JsonReader
    {
        private readonly Stack<CurrentItem> _items = new Stack<CurrentItem>();
        private readonly Stack<BlittableJsonReaderObject.PropertiesInsertionBuffer> _buffers = new Stack<BlittableJsonReaderObject.PropertiesInsertionBuffer>();

        private class CurrentItem
        {
            public BlittableJsonReaderObject.PropertiesInsertionBuffer Buffers;
            public BlittableJsonReaderObject Object;
            public BlittableJsonReaderArray Array;
            public int Position;
            public BlittableJsonReaderObject.PropertyDetails PropertyDetails;
        }

        private readonly Action<JsonReader, State> _setState = CreateFieldSetter<JsonReader, State>("_currentState");
        private readonly Action<JsonReader, JsonToken> _setToken = CreateFieldSetter<JsonReader, JsonToken>("_tokenType");

        public static Action<TClass, TField> CreateFieldSetter<TClass, TField>(string fieldName)
        {
            FieldInfo field = typeof(TClass).GetField(fieldName);
            ParameterExpression targetExp = Expression.Parameter(typeof(TField), "target");
            ParameterExpression valueExp = Expression.Parameter(typeof(TField), "value");

            MemberExpression fieldExp = Expression.Field(targetExp, field);
            BinaryExpression assignExp = Expression.Assign(fieldExp, valueExp);

            var setter = Expression.Lambda<Action<TClass, TField>>
                (assignExp, targetExp, valueExp).Compile();

            return setter;
        }

        public void Init(BlittableJsonReaderObject root)
        {
            _items.Clear();

            _setState(this, State.Start);
            _setToken(this, JsonToken.None);

            _items.Push(new CurrentItem
            {
                Object = root,
            });
        }

        private BlittableJsonReaderObject.PropertiesInsertionBuffer GetPropertiesInsertionBuffer()
        {
            if (_buffers.Count > 0)
                return _buffers.Pop();
            return new BlittableJsonReaderObject.PropertiesInsertionBuffer();
        }

        public override bool Read()
        {
            if (_items.Count == 0)
                return false;

            var current = _items.Peek();

            if (current.Object != null)
            {
                if (current.Buffers == null)
                {
                    current.Buffers = GetPropertiesInsertionBuffer();
                    current.Object.GetPropertiesByInsertionOrder(current.Buffers);
                    SetToken(JsonToken.StartObject);
                    return true;
                }
                if (current.Object.Modifications != null && current.Object.Modifications.Properties.Count > 0)
                {
                    Tuple<string, object> property;
                    if (CurrentState != State.Property)
                    {
                        property = current.Object.Modifications.Properties.Peek();
                        SetToken(JsonToken.PropertyName, property.Item1);
                        return true;
                    }
                    property = current.Object.Modifications.Properties.Dequeue(); // move to next property
                    return SetToken(GetTokenFromType(property.Item2), property.Item2);
                }
                if (current.Position == current.Object.Count)
                {
                    SetToken(JsonToken.EndObject);
                    _buffers.Push(current.Buffers);
                    _items.Pop();
                    return true;
                }
                if (CurrentState != State.Property)
                {
                    current.Object.GetPropertyByIndex(current.Buffers.Properties[current.Position],
                        ref current.PropertyDetails);
                    SetToken(JsonToken.PropertyName, current.PropertyDetails.Name.ToString());
                    return true;
                }
                current.Position++; // move to next property
                return SetToken(current.PropertyDetails.Token, current.PropertyDetails.Value);
            }
            if (current.Array != null)
            {
                if (current.Position == current.Array.Length)
                {
                    SetToken(JsonToken.EndArray);
                    _items.Pop();
                    return true;
                }

                var tuple = current.Array.GetValueTokenTupleByIndex(current.Position++);
                return SetToken(tuple.Item2, tuple.Item1);
            }

            throw new InvalidOperationException("Shouldn't happen");
        }

        private BlittableJsonToken GetTokenFromType(object val)
        {
            if (val is string)
                return BlittableJsonToken.String;
            if (val is bool)
                return BlittableJsonToken.Boolean;
            if (val == null)
                return BlittableJsonToken.Null;
            if (val is int || val is long)
                return BlittableJsonToken.Integer;
            if (val is float || val is double || val is decimal)
                return BlittableJsonToken.Float;
            if (val is IEnumerable)
                return BlittableJsonToken.StartArray;
            return BlittableJsonToken.StartObject;
        }

        private bool SetToken(BlittableJsonToken token, object value)
        {
            switch (token & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.StartObject:
                    var newObject = new CurrentItem
                    {
                        Object = (BlittableJsonReaderObject)value,
                        Buffers = GetPropertiesInsertionBuffer()
                    };
                    _items.Push(newObject);
                    newObject.Object.GetPropertiesByInsertionOrder(newObject.Buffers);
                    SetToken(JsonToken.StartObject);
                    return true;
                case BlittableJsonToken.StartArray:
                    var newArray = new CurrentItem
                    {
                        Array = (BlittableJsonReaderArray)value,
                    };
                    _items.Push(newArray);
                    SetToken(JsonToken.StartArray);
                    return true;
                case BlittableJsonToken.Integer:
                    SetToken(JsonToken.Integer, (long)value);
                    return true;
                case BlittableJsonToken.Float:
                    SetToken(JsonToken.Float, (double)((LazyDoubleValue)value));
                    return true;
                case BlittableJsonToken.String:
                case BlittableJsonToken.CompressedString:
                    SetToken(JsonToken.String, value.ToString());
                    return true;
                case BlittableJsonToken.Boolean:
                    SetToken(JsonToken.Boolean, (bool)value);
                    return true;
                case BlittableJsonToken.Null:
                    SetToken(JsonToken.Null);
                    return true;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public override int? ReadAsInt32()
        {
            if (!Read())
            {
                SetToken(JsonToken.None);
                return null;
            }
            if (Value is int)
                return (int)Value;
            return (int)Convert.ChangeType(Value, typeof(int));
        }

        public override string ReadAsString()
        {
            if (!Read())
            {
                SetToken(JsonToken.None);
                return null;
            }
            return Value?.ToString();
        }

        public override byte[] ReadAsBytes()
        {
            var str = ReadAsString();
            if (str == null)
                return null;
            return Convert.FromBase64String(str);
        }

        public override decimal? ReadAsDecimal()
        {
            if (!Read())
            {
                SetToken(JsonToken.None);
                return null;
            }
            if (Value is double)
                return (decimal)(double)Value;
            if (Value is decimal)
                return (decimal)Value;
            return (decimal)Convert.ChangeType(Value, typeof(decimal));
        }

        public override DateTime? ReadAsDateTime()
        {
            var str = ReadAsString();
            if (str == null)
                return null;
            return DateTime.ParseExact(str, "o", CultureInfo.InvariantCulture);
        }

        public override DateTimeOffset? ReadAsDateTimeOffset()
        {
            var str = ReadAsString();
            if (str == null)
                return null;
            return DateTime.ParseExact(str, "o", CultureInfo.InvariantCulture);
        }
    }

}