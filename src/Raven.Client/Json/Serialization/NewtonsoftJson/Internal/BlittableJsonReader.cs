using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using Newtonsoft.Json;
using Sparrow;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal
{
    internal class BlittableJsonReader : JsonReader, IJsonReader
    {
        private readonly Stack<CurrentItem> _items = new Stack<CurrentItem>();

        private class CurrentItem
        {
            public BlittableJsonReaderObject.InsertionOrderProperties? Buffers;
            public BlittableJsonReaderObject Object;
            public BlittableJsonReaderArray Array;
            public int Position;
            public BlittableJsonReaderObject.PropertyDetails PropertyDetails;
        }

        private readonly Action<JsonReader, State> _setState = ExpressionHelper.CreateFieldSetter<JsonReader, State>("_currentState");
        private readonly Action<JsonReader, JsonToken> _setToken = ExpressionHelper.CreateFieldSetter<JsonReader, JsonToken>("_tokenType");

        public readonly JsonOperationContext Context;

        public BlittableJsonReader()
        {
        }

        public BlittableJsonReader(JsonOperationContext context)
        {
            Context = context;
        }

        public void Initialize(BlittableJsonReaderObject root)
        {
            _items.Clear();

            _setState(this, State.Start);
            _setToken(this, JsonToken.None);

            _items.Push(new CurrentItem
            {
                Object = root
            });
        }

        private bool _readAsLazyNumber = false;

        public override unsafe bool Read()
        {
            if (_items.Count == 0)
                return false;

            var current = _items.Peek();

            if (current.Object != null)
            {
                while (true)
                {
                    if (current.Buffers == null)
                    {
                        current.Buffers = current.Object.GetPropertiesByInsertionOrder();
                        SetToken(JsonToken.StartObject);
                        return true;
                    }
                    var modifications = current.Object.Modifications;
                    if (modifications != null && modifications.ModificationsIndex < modifications.Properties.Count)
                    {
                        (string Name, object Value) property;
                        if (CurrentState != State.Property)
                        {
                            property = modifications.Properties[modifications.ModificationsIndex];
                            SetToken(JsonToken.PropertyName, property.Item1);
                            return true;
                        }
                        property = modifications.Properties[modifications.ModificationsIndex++]; // move to next property
                        return SetToken(GetTokenFromType(property.Item2), property.Item2);
                    }
                    if (current.Position == current.Object.Count)
                    {
                        SetToken(JsonToken.EndObject);
                        current.Buffers.Value.Dispose();
                        _items.Pop();
                        return true;
                    }
                    if (CurrentState != State.Property)
                    {
                        int propIndex = current.Buffers.Value.Properties[current.Position];
                        current.Object.GetPropertyByIndex(propIndex,
                            ref current.PropertyDetails);
                        if (modifications?.Removals?.Contains(propIndex) == true)
                        {
                            current.Position++; // skip removed property
                            continue; // now move to the next property...
                        }
                        SetToken(JsonToken.PropertyName, current.PropertyDetails.Name.ToString());
                        return true;
                    }
                    current.Position++; // move to next property
                    return SetToken(current.PropertyDetails.Token, current.PropertyDetails.Value);
                }
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
                case BlittableJsonToken.EmbeddedBlittable:
                    SetToken(JsonToken.Raw, value);
                    return true;
                case BlittableJsonToken.StartObject:
                    var obj = (BlittableJsonReaderObject)value;
                    var newObject = new CurrentItem
                    {
                        Object = (BlittableJsonReaderObject)value,
                        Buffers = obj.GetPropertiesByInsertionOrder()
                    };
                    _items.Push(newObject);

                    //The value is passed in case the field/property should remains BlittableJsonReaderObject
                    SetToken(JsonToken.StartObject, value);
                    return true;
                case BlittableJsonToken.StartArray:
                    var newArray = new CurrentItem
                    {
                        Array = (BlittableJsonReaderArray)value
                    };
                    _items.Push(newArray);

                    //The value is passed in case the field/property should remains BlittableJsonReaderArray
                    SetToken(JsonToken.StartArray, value);
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
            if (TokenType == JsonToken.StartObject)
                return null;

            var val = Value;
            if (val == null)
                return null;

            return Convert.FromBase64String(val.ToString());
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

        public void SkipBlittableInside()
        {
            SetToken(JsonToken.EndObject);
            _items.Pop();
        }

        public void SkipBlittableArrayInside()
        {
            SetToken(JsonToken.EndArray);
            _items.Pop();
        }

        void IDisposable.Dispose()
        {
        }
    }
}
