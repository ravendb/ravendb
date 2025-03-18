using System;
using CloudNative.CloudEvents;
using System.Text.Json;
using System.Text.Json.Serialization;
using CloudNative.CloudEvents.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public sealed class CloudEventConverter : JsonConverter<CloudEvent>
{
    public static readonly CloudEventConverter Instance = new();

    const string SpecVersionAttributeName = "specversion";

    private CloudEventConverter()
    {
    }

    public override void Write(Utf8JsonWriter writer, CloudEvent cloudEvent, JsonSerializerOptions options)
    {
        writer.WriteStartObject();

        writer.WritePropertyName(SpecVersionAttributeName);
        writer.WriteStringValue(cloudEvent.SpecVersion.VersionId);

        foreach (var pair in cloudEvent.GetPopulatedAttributes())
        {
            var attribute = pair.Key;
            if (attribute == cloudEvent.SpecVersion.DataContentTypeAttribute ||
                attribute.Name == Partitioning.PartitionKeyAttribute.Name)
            {
                continue;
            }

            var value = attribute.Format(pair.Value);

            writer.WritePropertyName(attribute.Name);
            writer.WriteStringValue(value);
        }

        writer.WritePropertyName("data");
        writer.WriteRawValue(((BlittableJsonReaderObject)cloudEvent.Data).ToString());

        writer.WriteEndObject();
    }

    public override CloudEvent Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}
