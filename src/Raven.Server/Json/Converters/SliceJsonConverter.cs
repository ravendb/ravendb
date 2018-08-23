using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using Newtonsoft.Json;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Json.Converters
{
    internal sealed class SliceJsonConverter : RavenJsonConverter
    {
        public static readonly SliceJsonConverter Instance = new SliceJsonConverter();

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (!(writer is BlittableJsonWriter blittableJsonWriter))
            {
                throw new SerializationException(
                    $"Try to write {nameof(Slice)} property/field by {writer.GetType()} witch is unsuitable reader. Should use {nameof(BlittableJsonWriter)}");
            }

            if (!(value is Slice slice))
            {
                throw new SerializationException($"Try to write {value.GetType()} witch is not {nameof(Slice)} value with {nameof(SliceJsonConverter)}");
            }

            var buffer = new byte[slice.Size];
            slice.CopyTo(buffer);
            var strValue = Convert.ToBase64String(buffer);

            blittableJsonWriter.WriteValue(strValue);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (!(reader is BlittableJsonReader blittableReader))
            {
                throw new SerializationException(
                    $"Try to read {nameof(Slice)} property/field by {reader.GetType()} witch is unsuitable reader. Should use {nameof(BlittableJsonReader)}");
            }

            if (blittableReader.Value == null)
            {
                return null;
            }

            if (!(blittableReader.Value is string strValue))
            {
                throw new SerializationException($"Try to read {nameof(Slice)} from non string value");
            }

            //TODO To create slice by JsonOperationContext
            if (!(blittableReader.Context is DocumentsOperationContext context))
            {
                throw new SerializationException($"{nameof(DocumentsOperationContext)} must to be used for reading {nameof(Slice)}");
            }

            var buffer = Convert.FromBase64String(strValue);
            Slice.From(context.Allocator, buffer, ByteStringType.Immutable, out var slice);

            return slice;
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(Slice);
        }
    }
}
