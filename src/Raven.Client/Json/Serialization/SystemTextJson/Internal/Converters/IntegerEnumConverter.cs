using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class IntegerEnumConverter : JsonConverterFactory
    {
        public static readonly IntegerEnumConverter Instance = new();

        private IntegerEnumConverter()
        {
        }

        public override bool CanConvert(Type typeToConvert)
        {
            return typeToConvert.IsEnum && typeToConvert.IsDefined(typeof(JsonConverterAttribute), inherit: false) == false;
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            return (JsonConverter)Activator.CreateInstance(typeof(IntegerEnumConverterInner<>).MakeGenericType(typeToConvert));
        }

        private sealed class IntegerEnumConverterInner<T> : JsonConverter<T>
            where T : struct, Enum
        {
            private readonly Dictionary<string, T> _enumMemberValues = new(StringComparer.OrdinalIgnoreCase);

            private readonly Dictionary<string, string> _enumMemberValuesByName;

            private static readonly TypeCode UnderlyingTypeCode = Type.GetTypeCode(Enum.GetUnderlyingType(typeof(T)));

            private readonly bool _unsigned;

            public IntegerEnumConverterInner()
            {
                _enumMemberValuesByName = EnumMemberStringEnumConverter.GetEnumMemberValues(typeof(T));
                foreach (KeyValuePair<string, string> entry in _enumMemberValuesByName)
                    _enumMemberValues[entry.Value] = (T)Enum.Parse(typeof(T), entry.Key);

                _unsigned = UnderlyingTypeCode is TypeCode.Byte or TypeCode.UInt16 or TypeCode.UInt32 or TypeCode.UInt64;
            }

            public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                switch (reader.TokenType)
                {
                    case JsonTokenType.Number:
                        return FromInt64(_unsigned ? (long)reader.GetUInt64() : reader.GetInt64());
                    case JsonTokenType.String:
                        return Parse(reader.GetString());
                    default:
                        throw new JsonException($"Expected a number or a string for '{typeof(T).FullName}' but got {reader.TokenType}.");
                }
            }

            public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
            {
                if (UnderlyingTypeCode == TypeCode.UInt64)
                    writer.WriteNumberValue(Unsafe.As<T, ulong>(ref value));
                else
                    writer.WriteNumberValue(ToInt64(value));
            }

            private static T FromInt64(long value)
            {
                switch (UnderlyingTypeCode)
                {
                    case TypeCode.SByte:
                        sbyte int8 = (sbyte)value;
                        return Unsafe.As<sbyte, T>(ref int8);
                    case TypeCode.Byte:
                        byte uint8 = (byte)value;
                        return Unsafe.As<byte, T>(ref uint8);
                    case TypeCode.Int16:
                        short int16 = (short)value;
                        return Unsafe.As<short, T>(ref int16);
                    case TypeCode.UInt16:
                        ushort uint16 = (ushort)value;
                        return Unsafe.As<ushort, T>(ref uint16);
                    case TypeCode.Int32:
                        int int32 = (int)value;
                        return Unsafe.As<int, T>(ref int32);
                    case TypeCode.UInt32:
                        uint uint32 = (uint)value;
                        return Unsafe.As<uint, T>(ref uint32);
                    case TypeCode.Int64:
                        return Unsafe.As<long, T>(ref value);
                    case TypeCode.UInt64:
                        ulong uint64 = (ulong)value;
                        return Unsafe.As<ulong, T>(ref uint64);
                    default:
                        return (T)Enum.ToObject(typeof(T), value);
                }
            }

            private static long ToInt64(T value)
            {
                switch (UnderlyingTypeCode)
                {
                    case TypeCode.SByte:
                        return Unsafe.As<T, sbyte>(ref value);
                    case TypeCode.Byte:
                        return Unsafe.As<T, byte>(ref value);
                    case TypeCode.Int16:
                        return Unsafe.As<T, short>(ref value);
                    case TypeCode.UInt16:
                        return Unsafe.As<T, ushort>(ref value);
                    case TypeCode.Int32:
                        return Unsafe.As<T, int>(ref value);
                    case TypeCode.UInt32:
                        return Unsafe.As<T, uint>(ref value);
                    case TypeCode.Int64:
                        return Unsafe.As<T, long>(ref value);
                    default:
                        return Convert.ToInt64(value);
                }
            }

            public override T ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                return Parse(reader.GetString());
            }

            public override void WriteAsPropertyName(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
            {
                writer.WritePropertyName(ToPropertyName(value));
            }

            private string ToPropertyName(T value)
            {
                string name = value.ToString();
                if (_enumMemberValuesByName.Count == 0)
                    return name;

                string[] names = name.Split([", "], StringSplitOptions.None);
                for (int i = 0; i < names.Length; i++)
                {
                    if (_enumMemberValuesByName.TryGetValue(names[i], out string enumMemberValue))
                        names[i] = enumMemberValue;
                }

                return string.Join(", ", names);
            }

            private T Parse(string text)
            {
                if (_enumMemberValues.TryGetValue(text, out T value) || Enum.TryParse(text, ignoreCase: true, out value))
                    return value;

                if (_enumMemberValues.Count > 0 && text.IndexOf(',') >= 0)
                {
                    string[] names = text.Split(',');
                    for (int i = 0; i < names.Length; i++)
                    {
                        string name = names[i].Trim();
                        names[i] = _enumMemberValues.TryGetValue(name, out T member) ? member.ToString() : name;
                    }

                    if (Enum.TryParse(string.Join(", ", names), ignoreCase: true, out value))
                        return value;
                }

                throw new JsonException($"The JSON value '{text}' could not be converted to {typeof(T)}.");
            }
        }
    }
}
