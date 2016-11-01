using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Imports.Newtonsoft.Json;
using Sparrow.Json;

namespace Raven.Client.Json
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


        public BlittableJsonReader(BlittableJsonReaderObject reader)
        {
            _items.Push(new CurrentItem
            {
                Object = reader,
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
                if (current.Position == current.Object.Count)
                {
                    SetToken(JsonToken.EndObject);
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
                    SetToken(JsonToken.Float, (double)value);
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