using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class UIntPtrConverter : JsonConverter<UIntPtr>
    {
        public static readonly UIntPtrConverter Instance = new();

        private UIntPtrConverter()
        {
        }

        public override UIntPtr Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            reader.Skip();
            return UIntPtr.Zero;
        }

        public override void Write(Utf8JsonWriter writer, UIntPtr value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteEndObject();
        }
    }
}
