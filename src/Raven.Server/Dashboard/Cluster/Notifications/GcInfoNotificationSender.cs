using System;
using System.Threading;
using Raven.Server.NotificationCenter;

namespace Raven.Server.Dashboard.Cluster.Notifications;

public class GcInfoNotificationSender : AbstractClusterDashboardNotificationSender
{
    public GcInfoNotificationSender(int widgetId, ConnectedWatcher watcher, CancellationToken shutdown) : base(widgetId, watcher, shutdown)
    {
    }

    private long _lastEphemeralIndex = -1;
    private long _lastBackgroundIndex = -1;
    private long _lastFullBlockingIndex = -1;

    protected override TimeSpan NotificationInterval { get; } = TimeSpan.FromSeconds(3);

    private GcInfoPayload.GcMemoryInfo ExtractGcMemoryInfoPayload(GCMemoryInfo info)
    {
        return new GcInfoPayload.GcMemoryInfo
        {
            Index = info.Index,
            Compacted = info.Compacted,
            Concurrent = info.Concurrent,
            Generation = info.Generation,
            PauseTimePercentage = info.PauseTimePercentage,
            PauseDurationsInMs =
            [
                info.PauseDurations[0].TotalMilliseconds,
                info.PauseDurations[1].TotalMilliseconds
            ],
            TotalHeapSizeAfterBytes = info.HeapSizeBytes,
            Gen0HeapSize =
                new GcInfoPayload.GenerationInfoSize
                {
                    SizeBeforeBytes = info.GenerationInfo[0].SizeBeforeBytes, 
                    SizeAfterBytes = info.GenerationInfo[0].SizeAfterBytes,
                    FragmentationBeforeBytes = info.GenerationInfo[0].FragmentationBeforeBytes,
                    FragmentationAfterBytes = info.GenerationInfo[0].FragmentationAfterBytes,
                },
            Gen1HeapSize =
                new GcInfoPayload.GenerationInfoSize
                {
                    SizeBeforeBytes = info.GenerationInfo[1].SizeBeforeBytes, 
                    SizeAfterBytes = info.GenerationInfo[1].SizeAfterBytes,
                    FragmentationBeforeBytes = info.GenerationInfo[1].FragmentationBeforeBytes,
                    FragmentationAfterBytes = info.GenerationInfo[1].FragmentationAfterBytes,
                },
            Gen2HeapSize =
                new GcInfoPayload.GenerationInfoSize
                {
                    SizeBeforeBytes = info.GenerationInfo[2].SizeBeforeBytes, 
                    SizeAfterBytes = info.GenerationInfo[2].SizeAfterBytes,
                    FragmentationBeforeBytes = info.GenerationInfo[2].FragmentationBeforeBytes,
                    FragmentationAfterBytes = info.GenerationInfo[2].FragmentationAfterBytes,
                },
            LargeObjectHeapSize =
                new GcInfoPayload.GenerationInfoSize
                {
                    SizeBeforeBytes = info.GenerationInfo[3].SizeBeforeBytes, 
                    SizeAfterBytes = info.GenerationInfo[3].SizeAfterBytes,
                    FragmentationBeforeBytes = info.GenerationInfo[3].FragmentationBeforeBytes,
                    FragmentationAfterBytes = info.GenerationInfo[3].FragmentationAfterBytes,
                },
            PinnedObjectHeapSize = new GcInfoPayload.GenerationInfoSize
            {
                SizeBeforeBytes = info.GenerationInfo[4].SizeBeforeBytes, 
                SizeAfterBytes = info.GenerationInfo[4].SizeAfterBytes,
                FragmentationBeforeBytes = info.GenerationInfo[4].FragmentationBeforeBytes,
                FragmentationAfterBytes = info.GenerationInfo[4].FragmentationAfterBytes,
            },
        };
    }

    protected override AbstractClusterDashboardNotification CreateNotification()
    {
        var ephemeralGc = GC.GetGCMemoryInfo(GCKind.Ephemeral);
        var backgroundGc = GC.GetGCMemoryInfo(GCKind.Background);
        var fullBlockingGc = GC.GetGCMemoryInfo(GCKind.FullBlocking);

        var payload = new GcInfoPayload
        {
            Ephemeral = _lastEphemeralIndex != ephemeralGc.Index ? ExtractGcMemoryInfoPayload(ephemeralGc) : null,
            Background = _lastBackgroundIndex != backgroundGc.Index ? ExtractGcMemoryInfoPayload(backgroundGc) : null,
            FullBlocking = _lastFullBlockingIndex != fullBlockingGc.Index ? ExtractGcMemoryInfoPayload(fullBlockingGc) : null,
        };

        _lastEphemeralIndex = ephemeralGc.Index;
        _lastBackgroundIndex = backgroundGc.Index;
        _lastFullBlockingIndex = fullBlockingGc.Index;

        return payload;
    }
}
