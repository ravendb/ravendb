namespace Raven.Server.EventListener;

public enum EventType
{
    GC,
    GCSuspend,
    GCRestart,
    GCFinalizers,
    Allocations,
    Contention,
    ThreadPoolWorkerThreadStart,
    ThreadPoolWorkerThreadWait,
    ThreadPoolWorkerThreadStop,
    ThreadPoolMinMaxThreads,
    ThreadPoolWorkerThreadAdjustment,
    ThreadPoolWorkerThreadAdjustmentSample,
    ThreadPoolWorkerThreadAdjustmentStats,
    ThreadCreating,
    ThreadCreated,
    ThreadRunning,
    GCCreateConcurrentThread_V1
}
