using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using Sparrow;

namespace Raven.Server.EventListener;

public class AllocationsHandler : AbstractEventsHandler<AllocationsHandler.AllocationInfo>
{
    protected override HashSet<EventType> DefaultEventTypes => new HashSet<EventType>(EventListenerToLog.AllocationEvents);

    protected override Action<AllocationInfo> OnEvent { get; }


    public AllocationsHandler(Action<AllocationInfo> onEvent, HashSet<EventType> eventTypes = null, long minimumDurationInMs = 0)
    {
        Update(eventTypes, minimumDurationInMs);
        OnEvent = onEvent;
    }

    public override bool HandleEvent(EventWrittenEventArgs eventData)
    {
        switch (eventData.EventName)
        {
            case EventListener.Constants.EventNames.Allocations.Allocation:
                if (EventTypes.Contains(EventType.Allocations) == false)
                    return true;

                var type = (string)eventData.Payload[5];
                var allocations = (ulong)eventData.Payload[3];
                var smallObjectAllocation = (uint)eventData.Payload[1] == 0x0;

                var allocation = new AllocationInfo
                {
                    AllocationType = type
                };

                if (smallObjectAllocation)
                {
                    allocation.SmallObjectAllocations = allocations;
                    allocation.NumberOfSmallObjectAllocations++;
                }
                else
                {
                    allocation.LargeObjectAllocations = allocations;
                    allocation.NumberOfLargeObjectAllocations++;
                }

                OnEvent.Invoke(allocation);

                return true;
        }

        return false;
    }

    public class AllocationInfo : Event
    {
        public AllocationInfo() : base(EventType.Allocations)
        {
        }

        private ulong? _allocations;

        public string AllocationType;
        public ulong SmallObjectAllocations;
        public ulong LargeObjectAllocations;
        public long NumberOfSmallObjectAllocations;
        public long NumberOfLargeObjectAllocations;

        public ulong Allocations
        {
            get
            {
                // used for ordering
                _allocations ??= SmallObjectAllocations + LargeObjectAllocations;
                return _allocations.Value;
            }
        }

        public long NumberOfAllocations => NumberOfSmallObjectAllocations + NumberOfLargeObjectAllocations;

        public override string ToString()
        {
            return $"type: {AllocationType}, allocations: {new Size((long)Allocations, SizeUnit.Bytes)}, count: {NumberOfAllocations}, " +
                   $"small: {new Size((long)SmallObjectAllocations, SizeUnit.Bytes)}, large: {new Size((long)LargeObjectAllocations, SizeUnit.Bytes)}";
        }
    }
}
