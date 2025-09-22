namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory;

internal class GcInfoPerGcKind
{
    public GcRunInfo Any { get; set; }
    public GcRunInfo Background { get; set; }
    public GcRunInfo Ephemeral { get; set; }
    public GcRunInfo FullBlocking { get; set; }
}
