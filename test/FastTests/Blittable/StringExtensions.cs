using System.IO;
using Newtonsoft.Json;

namespace FastTests.Blittable
{
    public static class StringExtensions
    {
        public static string ToJsonString(this object self)
        {
            var jsonSerializer = new JsonSerializer();
            var stringWriter = new StringWriter();
            var jsonWriter = new JsonTextWriter(stringWriter);
            jsonSerializer.Serialize(jsonWriter, self);

            return stringWriter.ToString();
        }
    }
}
