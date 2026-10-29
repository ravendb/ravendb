using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.AI;

// JSON a provider parses to build the request (tool-call arguments, the output schema), kept for one conversation call so
// it's parsed once instead of on every model iteration. It lives in the conversation call's context, not the per-write one.
public sealed class ParsedJsonCache(JsonOperationContext context)
{
    private readonly Dictionary<string, (string Json, BlittableJsonReaderObject Parsed)> _toolArguments = new();
    private (string Json, BlittableJsonReaderObject Parsed) _schema;

    internal int ParseCount;

    // The stored JSON is compared too, so calls that share an id (even an empty one) never get each other's arguments.
    public BlittableJsonReaderObject GetOrParseToolArguments(string toolCallId, string arguments)
    {
        var key = toolCallId ?? string.Empty;
        if (_toolArguments.TryGetValue(key, out var cached) && cached.Json == arguments)
            return cached.Parsed;

        var parsed = Parse(arguments);
        _toolArguments[key] = (arguments, parsed);
        return parsed;
    }

    public BlittableJsonReaderObject GetOrParseSchema(string schema)
    {
        if (_schema.Parsed == null || _schema.Json != schema)
            _schema = (schema, Parse(schema));

        return _schema.Parsed;
    }

    // Empty JSON (a tool without parameters) parses to an empty object.
    public static BlittableJsonReaderObject ParseObject(JsonOperationContext ctx, string json) =>
        string.IsNullOrWhiteSpace(json)
            ? ctx.ReadObject(new DynamicJsonValue(), "empty/json")
            : ctx.Sync.ReadForMemory(json, "json");

    private BlittableJsonReaderObject Parse(string json)
    {
        ParseCount++;
        return ParseObject(context, json);
    }
}
