using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http;

namespace Raven.AiAppliance.Endpoints.Helpers;

/// <summary>
/// Shared NDJSON (newline-delimited JSON) response helper. Each call to
/// <see cref="WriteLineAsync"/> serializes one self-contained object followed
/// by a newline and flushes immediately, so the client sees chunks as they
/// arrive. Used by every chat-style streaming endpoint (the embed chat, the
/// wizard <c>/setup/try</c> smoke test, and the legacy <c>/api/chat/stream</c>).
///
/// NDJSON over <c>application/x-ndjson</c> rather than SSE because the chat
/// frames carry a JSON body and the client POSTs the prompt (EventSource is
/// GET-only).
/// </summary>
internal static class NdjsonStream
{
    // Mirrors the bridge's global HTTP JSON options (Program.cs
    // ConfigureHttpJsonOptions): camelCase + string enums, so the streamed
    // chat frames serialize identically to every other bridge API response.
    // This is the bridge's wire convention (STJ/camelCase, what the widget JS
    // reads as msg.type / answer.reply) — deliberately NOT RavenDB's document
    // conventions (Newtonsoft/PascalCase), which would break the client.
    public static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        // Enums as their string name (e.g. "IFrame", "Done"), matching the
        // global JsonStringEnumConverter the rest of the bridge applies.
        Converters = { new JsonStringEnumConverter() },
        // Demo answer types use public fields, not properties, so the RavenDB
        // schema generator can read the initializers; STJ needs opt-in. The
        // default JavaScriptEncoder also escapes HTML-sensitive characters
        // (a literal '<' becomes <, decoded back by JSON.parse), avoiding
        // XSS exposure if chat output is ever embedded into an HTML context.
        IncludeFields = true,
    };

    /// <summary>Sets the streaming response headers. Call once before the first
    /// <see cref="WriteLineAsync"/>.</summary>
    public static void SetHeaders(HttpContext ctx)
    {
        ctx.Response.ContentType = "application/x-ndjson; charset=utf-8";
        ctx.Response.Headers["Cache-Control"] = "no-cache";
        ctx.Response.Headers["X-Accel-Buffering"] = "no";
    }

    /// <summary>Serializes <paramref name="payload"/> as one JSON line and flushes.</summary>
    public static async Task WriteLineAsync(HttpContext ctx, object payload)
    {
        var json = JsonSerializer.Serialize(payload, JsonOpts);
        var bytes = Encoding.UTF8.GetBytes(json + "\n");
        await ctx.Response.Body.WriteAsync(bytes, ctx.RequestAborted);
        await ctx.Response.Body.FlushAsync(ctx.RequestAborted);
    }
}
