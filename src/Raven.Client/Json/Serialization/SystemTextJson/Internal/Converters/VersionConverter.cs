using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class VersionConverter : JsonConverter<Version>
    {
        public static readonly VersionConverter Instance = new();

        private VersionConverter()
        {
        }

        public override Version Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
                return Version.Parse(reader.GetString());

            if (reader.TokenType != JsonTokenType.StartObject)
                throw new JsonException($"Expected a string or an object for '{typeof(Version).FullName}' but got {reader.TokenType}.");

            int major = 0, minor = 0, build = -1, revision = -1;
            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
            {
                string name = reader.GetString();
                reader.Read();

                if (string.Equals(name, nameof(Version.Major), StringComparison.OrdinalIgnoreCase))
                    major = reader.GetInt32();
                else if (string.Equals(name, nameof(Version.Minor), StringComparison.OrdinalIgnoreCase))
                    minor = reader.GetInt32();
                else if (string.Equals(name, nameof(Version.Build), StringComparison.OrdinalIgnoreCase))
                    build = reader.GetInt32();
                else if (string.Equals(name, nameof(Version.Revision), StringComparison.OrdinalIgnoreCase))
                    revision = reader.GetInt32();
                else
                    reader.Skip();
            }

            if (build < 0)
                return new Version(major, minor);

            return revision < 0 ? new Version(major, minor, build) : new Version(major, minor, build, revision);
        }

        public override void Write(Utf8JsonWriter writer, Version value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value.ToString());
        }
    }
}
