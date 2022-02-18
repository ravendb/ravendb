using System.IO;
using Newtonsoft.Json;

namespace Raven.Server.Utils;

public class JsonStringHelper
{
    public static string Indent(string json)
    {
        using (var stringReader = new StringReader(json))
        using (var stringWriter = new StringWriter())
        {
            var jsonReader = new JsonTextReader(stringReader);
            var jsonWriter = new JsonTextWriter(stringWriter) {Formatting = Formatting.Indented};
            jsonWriter.WriteToken(jsonReader);
            return stringWriter.ToString();
        }
    }
}
