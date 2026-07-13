using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Quill.Contracts;

/// <summary>Time-bucket granularity for <see cref="AppUsageResponse"/>. Serialized
/// as the prototype's lowercase wire values (<c>"hour"</c>/<c>"day"</c>) via
/// <see cref="UsageGranularityConverter"/> — without flipping the global PascalCase
/// enum policy (which the appliance keeps for Studio-paste compatibility).</summary>
[JsonConverter(typeof(UsageGranularityConverter))]
public enum UsageGranularity
{
    Hour,
    Day,
}

internal sealed class UsageGranularityConverter : JsonConverter<UsageGranularity>
{
    public override UsageGranularity Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        Enum.Parse<UsageGranularity>(reader.GetString() ?? nameof(UsageGranularity.Day), ignoreCase: true);

    public override void Write(Utf8JsonWriter writer, UsageGranularity value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value.ToString().ToLowerInvariant());
}
