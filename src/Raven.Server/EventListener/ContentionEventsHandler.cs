using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using Sparrow.Json.Parsing;

namespace Raven.Server.EventListener;

public class ContentionEventsHandler : AbstractEventsHandler<ContentionEventsHandler.ContentionEvent>
{
    protected override HashSet<EventType> DefaultEventTypes => EventListenerToLog.ContentionEvents;

    protected override Action<ContentionEvent> OnEvent { get; }

    private readonly Dictionary<Guid, DateTime> _contentionStarts = new ();

    public ContentionEventsHandler(Action<ContentionEvent> onEvent, HashSet<EventType> eventTypes = null, long minimumDurationInMs = 0)
    {
        Update(eventTypes, minimumDurationInMs);
        OnEvent = onEvent;
    }

    public override bool HandleEvent(EventWrittenEventArgs eventData)
    {
        switch (eventData.EventName)
        {
            case EventListener.Constants.EventNames.Contention.ContentionStart:
                if (EventTypes.Contains(EventType.Contention))
                    _contentionStarts[eventData.ActivityId] = DateTime.UtcNow;

                return true;

            case EventListener.Constants.EventNames.Contention.ContentionStop:
                if (EventTypes.Contains(EventType.Contention) &&
                    _contentionStarts.TryGetValue(eventData.ActivityId, out var startTime))
                {
                    var duration = (double)eventData.Payload[2] / 1_000_000.0;
                    if (duration >= MinimumDurationInMs)
                        OnEvent.Invoke(new ContentionEvent(startTime, duration));

                    _contentionStarts.Remove(eventData.ActivityId);
                }

                return true;
        }

        return false;
    }

    public class ContentionEvent : Event, IDurationEvent
    {
        public DateTime StartTime;

        public double DurationInMs { get; }

        public ContentionEvent(DateTime startTime, double durationInMs) : base(EventType.Contention)
        {
            StartTime = startTime;
            DurationInMs = durationInMs;
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(StartTime)] = StartTime;
            json[nameof(DurationInMs)] = DurationInMs;

            return json;
        }

        public override string ToString()
        {
            var str = base.ToString();
            return $"{str}, start time: {StartTime}, duration: {DurationInMs}ms";
        }
    }
}
