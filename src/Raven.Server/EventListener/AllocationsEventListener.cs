using System.Collections.Generic;
using System.Diagnostics.Tracing;

namespace Raven.Server.EventListener;

public class AllocationsEventListener : AbstractEventListener
{
    private readonly Dictionary<string, AllocationsHandler.AllocationInfo> _allocations = new();
    private readonly AllocationsHandler _handler;

    public IReadOnlyCollection<AllocationsHandler.AllocationInfo> Allocations => _allocations.Values;

    public AllocationsEventListener()
    {
        _handler = new AllocationsHandler(e =>
        {
            if (_allocations.TryGetValue(e.AllocationType, out var allocation) == false)
            {
                _allocations[e.AllocationType] = e;
                return;
            }

            allocation.SmallObjectAllocations += e.SmallObjectAllocations;
            allocation.NumberOfSmallObjectAllocations += e.NumberOfSmallObjectAllocations;
            allocation.LargeObjectAllocations += e.LargeObjectAllocations;
            allocation.NumberOfLargeObjectAllocations += e.NumberOfLargeObjectAllocations;
        });

        EnableEvents(DotNetEventType.GC);
    }

    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        if (eventData.EventName == null)
            return;

        _handler.HandleEvent(eventData);
    }
}
