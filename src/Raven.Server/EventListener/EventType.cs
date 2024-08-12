namespace Raven.Server.EventListener;

public enum EventType
{
    GC,
    GCSuspend,
    GCRestart,
    GCFinalizers,
    Contention,
    Allocations
}
