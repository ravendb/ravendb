using System;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide
{
    public class ServerStatistics
    {
        public ServerStatistics()
        {
            StartUpTime = SystemTime.UtcNow;
        }

        public TimeSpan UpTime => SystemTime.UtcNow - StartUpTime;

        public readonly DateTime StartUpTime;

        public DateTime? LastRequestTime { get; set; }

        public void WriteTo(BlittableJsonTextWriter writer)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(UpTime));
            writer.WriteString(UpTime.ToString("c"));
            writer.WriteComma();

            writer.WritePropertyName(nameof(StartUpTime));
            writer.WriteDateTime(StartUpTime, isUtc: true);
            writer.WriteComma();

            writer.WritePropertyName(nameof(LastRequestTime));
            if (LastRequestTime.HasValue)
                writer.WriteDateTime(LastRequestTime.Value, isUtc: true);
            else
                writer.WriteNull();

            writer.WriteEndObject();
        }
    }
}
