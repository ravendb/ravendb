using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public static class BlittableJsonTextWriterExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteArray<T>(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, string name, IEnumerable<T> items,
            Action<AbstractBlittableJsonTextWriter, JsonOperationContext, T> onWrite)
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
        public static void WriteArray(this AbstractBlittableJsonTextWriter writer, string name, Memory<double> items)
        {
            writer.WritePropertyName(name);

            writer.WriteStartArray();
            for (int i = 0; i < items.Length; i++)
            {
                if (i > 0)
                    writer.WriteComma();
                writer.WriteDouble(items.Span[i]);
            }
            writer.WriteEndArray();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteArray(this AbstractBlittableJsonTextWriter writer, string name, IEnumerable<LazyStringValue> items)
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
        public static void WriteArray(this AbstractBlittableJsonTextWriter writer, string name, IEnumerable<string> items)
        {
            writer.WritePropertyName(name);

            if (items == null)
            {
                writer.WriteNull();
                return;
            }

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
        public static void WriteArray(this AbstractBlittableJsonTextWriter writer, string name, IEnumerable<DynamicJsonValue> items, JsonOperationContext context)
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
        public static void WriteArray(this AbstractBlittableJsonTextWriter writer, string name, IEnumerable<BlittableJsonReaderObject> items)
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
