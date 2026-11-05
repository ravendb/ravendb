using System.Collections.Generic;
using System.Text.Encodings.Web;
using System.Text.Json;
using Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters;

namespace Raven.Client.Json
{
    internal static class JsonDynamicFormatter
    {
        private static readonly JsonSerializerOptions _options = new()
        {
            WriteIndented = true,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            Converters = { JsonDynamicConverters.Value }
        };

        public static string ToJson(IDictionary<string, object> value) => JsonSerializer.Serialize(value, _options);

        public static string ToJson(IList<object> value) => JsonSerializer.Serialize(value, _options);
    }
}
