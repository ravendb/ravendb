using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using Newtonsoft.Json;
using Sparrow;
using Sparrow.Json;

namespace Raven.Client.Json
{
    internal class BlittableJsonReader : JsonReader
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

        private readonly Action<JsonReader, State> _setState = ExpressionHelper.CreateFieldSetter<JsonReader, State>("_currentState");
        private readonly Action<JsonReader, JsonToken> _setToken = ExpressionHelper.CreateFieldSetter<JsonReader, JsonToken>("_tokenType");

        public void Init(BlittableJsonReaderObject root)
        {
            _items.Clear();

            _setState(this, State.Start);
            _setToken(this, JsonToken.None);

            _items.Push(new CurrentItem
            {
                Object = root
            });
        }

        private BlittableJsonReaderObject.PropertiesInsertionBuffer GetPropertiesInsertionBuffer()
        {
            if (_buffers.Count > 0)
                return _buffers.Pop();
            return new BlittableJsonReaderObject.PropertiesInsertionBuffer();
        }


        private bool _readAsLazyNumber = false;
       

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
                    (string Name, object Value) property;
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

        private static BlittableJsonToken GetTokenFromType(object val)
        {
            if (val is string || val is LazyStringValue)
                return BlittableJsonToken.String;
            if (val is LazyCompressedStringValue)
                return BlittableJsonToken.CompressedString;
            if (val is bool)
                return BlittableJsonToken.Boolean;
            if (val == null)
                return BlittableJsonToken.Null;
            if (val is int || val is long)
                return BlittableJsonToken.Integer;
            if (val is float || val is double || val is LazyNumberValue)
                return BlittableJsonToken.LazyNumber;
            if (val is IEnumerable)
                return BlittableJsonToken.StartArray;
            if (val is decimal asDec)
            {                
                return BlittableJsonToken.LazyNumber;
            }
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
                        Array = (BlittableJsonReaderArray)value
                    };
                    _items.Push(newArray);
                    SetToken(JsonToken.StartArray);
                    return true;
                case BlittableJsonToken.Integer:                  
                    SetToken(JsonToken.Integer, (long)value);
                    return true;
                case BlittableJsonToken.LazyNumber:
                    if (_readAsLazyNumber)
                    {
                        SetToken(JsonToken.Float, value);
                    }
                    else
                    {
                        LazyNumberValue lnv = (LazyNumberValue)value;
                        if (lnv.TryParseULong(out var ulongValue))
                        {
                            SetToken(JsonToken.Integer, ulongValue);
                        }
                        else if (lnv.TryParseDecimal(out var decimalValue))
                        {                            
                            SetToken(JsonToken.Float, decimalValue);
                        }
                        else
                        {
                            SetToken(JsonToken.Float, (double)lnv);
                        }
                        
                    }                    

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
            _readAsLazyNumber = true;
            try
            {
                if (!Read())
                {
                    SetToken(JsonToken.None);
                    return null;
                }
            }
            finally
            {
                _readAsLazyNumber = false;
            }

            if (Value is LazyNumberValue lazyNumber)
            {
                int numberAsInt;
                if (lazyNumber.TryParseULong(out var ulongValue) == false)
                {
                    numberAsInt = (int)(double)lazyNumber;
                }
                else
                {
                    numberAsInt = (int)ulongValue;
                }
                SetToken(JsonToken.Integer, numberAsInt);

                return numberAsInt;
            }
            else if (Value is int)
                return (int)Value;
            else
            {
                //This method will return null at the end of an array.
                if (TokenType == JsonToken.EndArray)
                    return new int?();

                if (TokenType == JsonToken.Null)
                    return new int?();
            }

            return (int)Convert.ChangeType(Value, typeof(int), CultureInfo.InvariantCulture);
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
            if (!Read())
            {
                SetToken(JsonToken.None);
                return null;
            }
            if (TokenType == JsonToken.StartObject || 
                TokenType == JsonToken.None || 
                TokenType == JsonToken.Null ||
                TokenType == JsonToken.Undefined)
                return null;

            return Convert.FromBase64String(Value.ToString());
        }

        public override decimal? ReadAsDecimal()
        {
            _readAsLazyNumber = true;
            try
            { 
                if (!Read())
                {
                    SetToken(JsonToken.None);
                    return null;
                }
            }
            finally
            {
                _readAsLazyNumber = false;
            }
            if (Value == null)
                return null;

            if (Value is LazyNumberValue lnv)
            {
                if (lnv.TryParseDecimal(out var decimalVal))
                {
                    SetToken(JsonToken.Float, decimalVal);
                    return decimalVal;
                }
            }

            return (decimal)Convert.ChangeType(Value, typeof(decimal), CultureInfo.InvariantCulture);            
        }

        public override double? ReadAsDouble()
        {
            _readAsLazyNumber = true;
            try
            {
                if (!Read())
                {
                    SetToken(JsonToken.None);
                    return null;
                }
            }
            finally
            {
                _readAsLazyNumber = false;
            }
            if (Value == null)
                return null;

            if (Value is LazyNumberValue lnv)
            {
                if (lnv.TryParseDouble(out var doubleVal))
                {
                    SetToken(JsonToken.Float, doubleVal);
                    return doubleVal;
                }
            }
           
            return (double)Convert.ChangeType(Value, typeof(double), CultureInfo.InvariantCulture);
        }                

        public override DateTime? ReadAsDateTime()
        {
            var str = ReadAsString();
            if (str == null)
                return null;
            return DateTime.ParseExact(str, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        }

        public override DateTimeOffset? ReadAsDateTimeOffset()
        {
            var str = ReadAsString();
            if (str == null)
                return null;
            return DateTimeOffset.ParseExact(str, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        }
    }

}
