using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

internal readonly struct StreamEventResult(LazyStringValue textDelta, bool stop)
{
    public readonly LazyStringValue TextDelta = textDelta;
    public readonly bool Stop = stop;               // true when the provider signals end-of-stream (e.g. message_stop)
}
