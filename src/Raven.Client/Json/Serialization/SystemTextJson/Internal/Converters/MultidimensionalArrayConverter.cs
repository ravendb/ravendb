using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class MultidimensionalArrayConverter : JsonConverterFactory
    {
        private readonly TypeNameHandling _typeNameHandling;

        public MultidimensionalArrayConverter(TypeNameHandling typeNameHandling)
        {
            _typeNameHandling = typeNameHandling;
        }

        public override bool CanConvert(Type typeToConvert)
        {
            return typeToConvert.IsArray && typeToConvert.GetArrayRank() > 1;
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            Type converterType = typeof(MultidimensionalArrayConverterInner<,>).MakeGenericType(typeToConvert, typeToConvert.GetElementType());
            return (JsonConverter)Activator.CreateInstance(converterType, _typeNameHandling);
        }

        private sealed class MultidimensionalArrayConverterInner<TArray, TElement>(TypeNameHandling typeNameHandling) : JsonConverter<TArray>
            where TArray : class
        {
            public override TArray Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                int rank = typeToConvert.GetArrayRank();
                List<object> items = ReadDimension(ref reader, 0, rank, typeNameHandling, options);

                var lengths = new int[rank];
                List<object> current = items;
                for (int dimension = 0; dimension < rank && current != null; dimension++)
                {
                    lengths[dimension] = current.Count;
                    current = dimension < rank - 1 && current.Count > 0 ? (List<object>)current[0] : null;
                }

                Array array = Array.CreateInstance(typeof(TElement), lengths);
                Fill(array, items, new int[rank], 0, lengths);
                return (TArray)(object)array;
            }

            private static List<object> ReadDimension(ref Utf8JsonReader reader, int dimension, int rank, TypeNameHandling typeNameHandling, JsonSerializerOptions options)
            {
                if (reader.TokenType != JsonTokenType.StartArray)
                    throw new JsonException($"Expected StartArray for dimension {dimension} of '{typeof(TArray).FullName}' but got {reader.TokenType}.");

                var items = new List<object>();
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (dimension < rank - 1)
                        items.Add(ReadDimension(ref reader, dimension + 1, rank, typeNameHandling, options));
                    else
                        items.Add(TypeNameConverter.ReadItem(ref reader, typeof(TElement), typeNameHandling, options));
                }

                return items;
            }

            private static void Fill(Array array, List<object> items, int[] indices, int dimension, int[] lengths)
            {
                if (items.Count != lengths[dimension])
                    throw new JsonException($"Cannot read a jagged JSON array into '{typeof(TArray).FullName}'.");

                for (int i = 0; i < items.Count; i++)
                {
                    indices[dimension] = i;
                    if (dimension == lengths.Length - 1)
                        array.SetValue(items[i], indices);
                    else
                        Fill(array, (List<object>)items[i], indices, dimension + 1, lengths);
                }
            }

            public override void Write(Utf8JsonWriter writer, TArray value, JsonSerializerOptions options)
            {
                var array = (Array)(object)value;
                WriteDimension(writer, array, new int[array.Rank], 0, typeNameHandling, options);
            }

            private static void WriteDimension(Utf8JsonWriter writer, Array array, int[] indices, int dimension, TypeNameHandling typeNameHandling, JsonSerializerOptions options)
            {
                writer.WriteStartArray();

                int upperBound = array.GetUpperBound(dimension);
                for (int i = array.GetLowerBound(dimension); i <= upperBound; i++)
                {
                    indices[dimension] = i;
                    if (dimension == array.Rank - 1)
                        TypeNameConverter.WriteItem(writer, array.GetValue(indices), typeof(TElement), typeNameHandling, options);
                    else
                        WriteDimension(writer, array, indices, dimension + 1, typeNameHandling, options);
                }

                writer.WriteEndArray();
            }
        }
    }
}
