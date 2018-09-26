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
    internal sealed class SliceJsonConverter : RavenTypeJsonConverter<Slice>
    {
        public static readonly SliceJsonConverter Instance = new SliceJsonConverter();

        private SliceJsonConverter() {}

        protected override void WriteJson(BlittableJsonWriter writer, Slice value, JsonSerializer serializer)
        {
            var buffer = new byte[value.Size];
            value.CopyTo(buffer);
            var strValue = Convert.ToBase64String(buffer);

            writer.WriteValue(strValue);
        }

        internal override Slice ReadJson(BlittableJsonReader reader)
        {
            if (!(reader.Value is string strValue))
            {
                throw new SerializationException($"Try to read {nameof(Slice)} from {reader.Value?.GetType()}. Should be string here");
            }

            if (!(reader.Context is DocumentsOperationContext context))
            {
                throw new SerializationException($"{nameof(DocumentsOperationContext)} must to be used for reading {nameof(Slice)}");
            }

            var buffer = Convert.FromBase64String(strValue);
            Slice.From(context.Allocator, buffer, ByteStringType.Immutable, out var slice);

            return slice;
        }
    }
}
