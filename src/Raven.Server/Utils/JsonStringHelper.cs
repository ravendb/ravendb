using System.IO;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Raven.Server.Utils;

public sealed class JsonStringHelper
{
    private static readonly JsonDocumentOptions ReadOptions = new()
    {
        CommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true
    };

    private static readonly JsonWriterOptions WriteOptions = new()
    {
        Indented = true,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    public static string Indent(string json)
    {
        using (JsonDocument document = JsonDocument.Parse(json, ReadOptions))
        using (var stream = new MemoryStream())
        {
            using (var writer = new Utf8JsonWriter(stream, WriteOptions))
                document.RootElement.WriteTo(writer);

            return Encoding.UTF8.GetString(stream.GetBuffer(), 0, (int)stream.Length);
        }
    }
}
