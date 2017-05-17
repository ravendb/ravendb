using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public static class BlittableJsonTextWriterExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteArray<T>(this BlittableJsonTextWriter writer, JsonOperationContext context, string name, IEnumerable<T> items, 
            Action<BlittableJsonTextWriter, JsonOperationContext, T> onWrite)
        {
            writer.WritePropertyName(name);

            writer.WriteStartArray();
            var first = true;
            foreach (var item in items)
            {
                if (first == false)
                    writer.WriteComma();

                first = false;

                onWrite(writer, context, item);
            }

            writer.WriteEndArray();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteArray(this BlittableJsonTextWriter writer, string name, IEnumerable<LazyStringValue> items)
        {
            writer.WritePropertyName(name);

            writer.WriteStartArray();
            var first = true;
            foreach (var item in items)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;

                writer.WriteString(item);
            }
            writer.WriteEndArray();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteArray(this BlittableJsonTextWriter writer, string name, IEnumerable<string> items)
        {
            writer.WritePropertyName(name);

            writer.WriteStartArray();
            var first = true;
            foreach (var item in items)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;

                writer.WriteString(item);
            }
            writer.WriteEndArray();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteArray(this BlittableJsonTextWriter writer, string name, IEnumerable<DynamicJsonValue> items, JsonOperationContext context)
        {
            writer.WritePropertyName(name);

            writer.WriteStartArray();
            var first = true;
            foreach (var item in items)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;

                context.Write(writer, item);
            }
            writer.WriteEndArray();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteArray(this BlittableJsonTextWriter writer, string name, IEnumerable<BlittableJsonReaderObject> items)
        {
            writer.WritePropertyName(name);

            writer.WriteStartArray();
            var first = true;
            foreach (var item in items)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;

                writer.WriteObject(item);
            }
            writer.WriteEndArray();
        }
    }
}