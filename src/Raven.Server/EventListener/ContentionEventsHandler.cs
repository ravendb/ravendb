using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using Sparrow.Json.Parsing;

namespace Raven.Server.EventListener;

public class ContentionEventsHandler : AbstractEventsHandler<ContentionEventsHandler.ContentionEvent>
{
    protected override Action<ContentionEvent> OnEvent { get; }

    private readonly Dictionary<Guid, DateTime> _contentionStarts = new ();

    public ContentionEventsHandler(Action<ContentionEvent> onEvent)
    {
        OnEvent = onEvent;
    }

    public override bool HandleEvent(EventWrittenEventArgs eventData)
    {
        switch (eventData.EventName)
        {
            case "ContentionStart":
                _contentionStarts[eventData.ActivityId] = DateTime.UtcNow;
                return true;

            case "ContentionStop":
                if (_contentionStarts.TryGetValue(eventData.ActivityId, out var startTime))
                {
                    OnEvent.Invoke(new ContentionEvent(startTime, (double)eventData.Payload[2] / 1_000_000.0));
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
    }
}
