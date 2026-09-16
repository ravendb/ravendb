using System.Text.Encodings.Web;
using System.Text.Json;

namespace Raven.Quill.Embed;

/// The host↔widget message contract. Mirrors `packages/widget/src/host-channel.ts`; this side exists
/// because the notice shells (expired, forbidden, unavailable) carry no widget bundle and still have to
/// tell the host page what happened.
internal static class HostChannel
{
    internal const string EnvelopeSource = "raven-quill";
    internal const int EnvelopeVersion = 1;

    private static readonly JsonSerializerOptions ScriptJson = new()
    {
        // escapes < > & so the serialized envelope can never terminate the <script> block
        Encoder = JavaScriptEncoder.Default,
    };

    /// Posted to `"*"`. Every message in this protocol is deliberately data-free — a type, and for an
    /// error a fixed English string — precisely so the target origin does not have to be known. The host
    /// page validates `source`/`version` on its end; nothing here is worth restricting to one origin, and
    /// a live embed cannot know its parent's origin anyway (a framed navigation carries no Origin header).
    internal static string BuildPostMessageScript(string type, object payload)
    {
        var envelope = JsonSerializer.Serialize(
            new { source = EnvelopeSource, version = EnvelopeVersion, type, payload }, ScriptJson);

        return $"if(window.parent!==window){{try{{window.parent.postMessage({envelope},\"*\")}}catch(e){{}}}}";
    }
}
