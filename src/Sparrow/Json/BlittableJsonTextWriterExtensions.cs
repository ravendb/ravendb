using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    internal static class BlittableJsonTextWriterExtensions
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
        public static void WriteArrayValue(this AbstractBlittableJsonTextWriter writer, IEnumerable<string> items)
        {
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
        public static async Task WriteArrayAsync(this AsyncBlittableJsonTextWriter writer, string name, IEnumerable<Stream> items)
        {
            writer.WritePropertyName(name);

            writer.WriteStartArray();
            var first = true;
            foreach (var item in items)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;

                item.Position = 0;
                await writer.WriteStreamAsync(item).ConfigureAwait(false);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async Task WriteArrayAsync(this AsyncBlittableJsonTextWriter writer, string name, IEnumerable<BlittableJsonReaderObject> items)
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
                await writer.MaybeOuterFlushAsync().ConfigureAwait(false);
            }
            writer.WriteEndArray();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async Task WriteArrayAsync(this AsyncBlittableJsonTextWriter writer, string name, IAsyncEnumerable<BlittableJsonReaderObject> items)
        {
            writer.WritePropertyName(name);

            writer.WriteStartArray();
            var first = true;
            await foreach (var item in items)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;

                writer.WriteObject(item);
                await writer.MaybeOuterFlushAsync().ConfigureAwait(false);
            }
            writer.WriteEndArray();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteArray(this AbstractBlittableJsonTextWriter writer, string name, IEnumerable<long> items)
        {
            writer.WritePropertyName(name);

            writer.WriteStartArray();
            var first = true;
            foreach (var item in items)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;

                writer.WriteInteger(item);
            }
            writer.WriteEndArray();
        }
    }
}
