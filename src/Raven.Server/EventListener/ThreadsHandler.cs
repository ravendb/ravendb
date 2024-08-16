using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics.Tracing;
using Sparrow.Json.Parsing;

namespace Raven.Server.EventListener;

public class ThreadsHandler : AbstractEventsHandler<ThreadsHandler.ThreadEvent>
{
    protected override HashSet<EventType> DefaultEventTypes => EventListenerToLog.ThreadEvents;

    protected override Action<ThreadEvent> OnEvent { get; }

    private ThreadPoolWorkerThreadWaitEvent _lastThreadPoolWorkerThreadWaitEvent;

    public ThreadsHandler(Action<ThreadEvent> onEvent, HashSet<EventType> eventTypes = null, long minimumDurationInMs = 0)
    {
        Update(eventTypes, minimumDurationInMs);
        OnEvent = onEvent;
    }

    public override bool HandleEvent(EventWrittenEventArgs eventData)
    {
        switch (eventData.EventName)
        {
            case EventListener.Constants.EventNames.Threads.ThreadCreating:
                if (EventTypes.Contains(EventType.ThreadCreating))
                {
                    OnEvent.Invoke(new ThreadCreationInfoEvent(EventType.ThreadCreating, eventData.Payload));
                }

                return true;

            case EventListener.Constants.EventNames.Threads.ThreadCreated:
                if (EventTypes.Contains(EventType.ThreadCreated))
                {
                    OnEvent.Invoke(new ThreadCreatedEvent(eventData.Payload));
                }

                return true;

            case EventListener.Constants.EventNames.Threads.ThreadRunning:
                if (EventTypes.Contains(EventType.ThreadRunning))
                {
                    OnEvent.Invoke(new ThreadCreationInfoEvent(EventType.ThreadRunning, eventData.Payload));
                }

                return true;

            case EventListener.Constants.EventNames.Threads.ThreadPoolWorkerThreadStart:
                if (EventTypes.Contains(EventType.ThreadPoolWorkerThreadStart))
                {
                    OnEvent.Invoke(new ThreadPoolWorkerThreadStartEvent(eventData.Payload));
                }

                return true;

            case EventListener.Constants.EventNames.Threads.ThreadPoolWorkerThreadWait:
                if (EventTypes.Contains(EventType.ThreadPoolWorkerThreadWait))
                {
                    var newWaitEvent = new ThreadPoolWorkerThreadWaitEvent(eventData.Payload);
                    if (_lastThreadPoolWorkerThreadWaitEvent == null ||
                        newWaitEvent.ActiveWorkerThreadCount != _lastThreadPoolWorkerThreadWaitEvent.ActiveWorkerThreadCount ||
                        newWaitEvent.RetiredWorkerThreadCount != _lastThreadPoolWorkerThreadWaitEvent.RetiredWorkerThreadCount)
                    {
                        _lastThreadPoolWorkerThreadWaitEvent = newWaitEvent;
                        OnEvent.Invoke(_lastThreadPoolWorkerThreadWaitEvent);
                    }
                }

                return true;

            case EventListener.Constants.EventNames.Threads.ThreadPoolWorkerThreadStop:
                if (EventTypes.Contains(EventType.ThreadPoolWorkerThreadStop))
                {
                    OnEvent.Invoke(new ThreadPoolWorkerThreadStopEvent(eventData.Payload));
                }

                return true;

            case EventListener.Constants.EventNames.Threads.ThreadPoolMinMaxThreads:
                if (EventTypes.Contains(EventType.ThreadPoolMinMaxThreads))
                {
                    OnEvent.Invoke(new ThreadPoolMinMaxThreadsEvent(eventData.Payload));
                }

                return true;
            
            case EventListener.Constants.EventNames.Threads.ThreadPoolWorkerThreadAdjustmentAdjustment:
                if (EventTypes.Contains(EventType.ThreadPoolWorkerThreadAdjustment))
                {
                    OnEvent.Invoke(new ThreadPoolWorkerThreadAdjustmentEvent(eventData.Payload));
                }

                return true;

            case EventListener.Constants.EventNames.Threads.ThreadPoolWorkerThreadAdjustmentSample:
                if (EventTypes.Contains(EventType.ThreadPoolWorkerThreadAdjustmentSample))
                {
                    OnEvent.Invoke(new ThreadPoolWorkerThreadAdjustmentSample(eventData.Payload));
                }

                return true;

            case EventListener.Constants.EventNames.Threads.ThreadPoolWorkerThreadAdjustmentStats:
                if (EventTypes.Contains(EventType.ThreadPoolWorkerThreadAdjustmentStats))
                {
                    OnEvent.Invoke(new ThreadPoolWorkerThreadAdjustmentStats(eventData.Payload));
                }

                return true;

            case EventListener.Constants.EventNames.Threads.GCCreateConcurrentThread_V1:
                if (EventTypes.Contains(EventType.GCCreateConcurrentThread_V1))
                {
                    OnEvent.Invoke(new ThreadEvent(EventType.GCCreateConcurrentThread_V1));
                }

                return true;

        }

        return false;
    }

    public class ThreadEvent : Event
    {
        public ThreadEvent(EventType type) : base(type)
        {
        }
    }

    public class ThreadCreationInfoEvent : ThreadEvent
    {
        public ulong InternalThreadId { get; }

        public ThreadCreationInfoEvent(EventType type, ReadOnlyCollection<object> payload) : base(type)
        {
            InternalThreadId = payload[0] is ulong ? (ulong)payload[0] : payload[0] is IntPtr ? (ulong)((IntPtr)payload[0]).ToInt64() : ulong.MinValue;
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(InternalThreadId)] = InternalThreadId;
            return json;
        }

        public override string ToString()
        {
            var str = base.ToString();
            return $"{str}, internal thread id: {InternalThreadId}";
        }
    }

    public class ThreadCreatedEvent : ThreadCreationInfoEvent
    {
        public ulong ManagedThreadId { get; }

        public ulong UnmanagedThreadId { get; }

        public ThreadCreatedEvent(ReadOnlyCollection<object> payload) : base(EventType.ThreadCreated, payload)
        {
            ManagedThreadId = (uint)payload[3];
            UnmanagedThreadId = (uint)payload[4];
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(ManagedThreadId)] = ManagedThreadId;
            json[nameof(UnmanagedThreadId)] = UnmanagedThreadId;
            return json;
        }

        public override string ToString()
        {
            var str = base.ToString();
            return $"{str}, managed thread id: {ManagedThreadId}, unmanaged thread id: {UnmanagedThreadId}";
        }
    }

    public abstract class ThreadPoolWorkerThreadWaitBaseEvent : ThreadEvent
    {
        public uint ActiveWorkerThreadCount { get; set; }

        public uint RetiredWorkerThreadCount { get; set; }

        protected ThreadPoolWorkerThreadWaitBaseEvent(EventType type, ReadOnlyCollection<object> payload) : base(type)
        {
            ActiveWorkerThreadCount = (uint)payload[0];
            RetiredWorkerThreadCount = (uint)payload[1];
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(ActiveWorkerThreadCount)] = ActiveWorkerThreadCount;
            json[nameof(RetiredWorkerThreadCount)] = RetiredWorkerThreadCount;
            return json;
        }

        public override string ToString()
        {
            var str = base.ToString();
            return $"{str}, active worker thread count: {ActiveWorkerThreadCount}, retired worker thread count: {RetiredWorkerThreadCount}";
        }
    }

    public class ThreadPoolWorkerThreadStartEvent : ThreadPoolWorkerThreadWaitBaseEvent
    {
        public ThreadPoolWorkerThreadStartEvent(ReadOnlyCollection<object> payload) : base(EventType.ThreadPoolWorkerThreadWait, payload)
        {
        }
    }

    public class ThreadPoolWorkerThreadWaitEvent : ThreadPoolWorkerThreadWaitBaseEvent
    {
        public ThreadPoolWorkerThreadWaitEvent(ReadOnlyCollection<object> payload) : base(EventType.ThreadPoolWorkerThreadWait, payload)
        {
        }
    }

    public class ThreadPoolWorkerThreadStopEvent : ThreadPoolWorkerThreadWaitBaseEvent
    {
        public ThreadPoolWorkerThreadStopEvent(ReadOnlyCollection<object> payload) : base(EventType.ThreadPoolWorkerThreadWait, payload)
        {
        }
    }

    public class ThreadPoolMinMaxThreadsEvent : ThreadEvent
    {
        public ushort MinWorkerThreads { get; }
        public ushort MaxWorkerThreads { get; }
        public ushort MinIoCompletionThreads { get; }
        public ushort MaxIoCompletionThreads { get; }

        public ThreadPoolMinMaxThreadsEvent(ReadOnlyCollection<object> payload) : base(EventType.ThreadPoolMinMaxThreads)
        {
            MinWorkerThreads = (ushort)payload[0];
            MaxWorkerThreads = (ushort)payload[1];
            MinIoCompletionThreads = (ushort)payload[2];
            MaxIoCompletionThreads = (ushort)payload[3];
        }
    }

    public class ThreadPoolWorkerThreadAdjustmentEvent : ThreadEvent
    {
        public double AverageThroughput { get; }
        public uint NewWorkerThreadCount { get; }
        public string Reason { get; }

        public ThreadPoolWorkerThreadAdjustmentEvent(ReadOnlyCollection<object> payload) : base(EventType.ThreadPoolWorkerThreadAdjustment)
        {
            AverageThroughput = (double)payload[0];
            NewWorkerThreadCount = (uint)payload[1];
            Reason = GetReason((uint)payload[2]);
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(AverageThroughput)] = AverageThroughput;
            json[nameof(NewWorkerThreadCount)] = NewWorkerThreadCount;
            json[nameof(Reason)] = Reason;
            return json;
        }

        public override string ToString()
        {
            var str = base.ToString();
            return $"{str}, average throughput: {AverageThroughput}, new worker thread count: {NewWorkerThreadCount}, reason: {Reason}";
        }

        private static string GetReason(uint valueReason)
        {
            switch (valueReason)
            {
                case 0x0:
                    return "Warmup";
                case 0x1:
                    return "Initializing";
                case 0x2:
                    return "Random move";
                case 0x3:
                    return "Climbing move";
                case 0x4:
                    return "Change point";
                case 0x5:
                    return "Stabilizing";
                case 0x6:
                    return "Starvation";
                case 0x7:
                    return "Thread timed out";
                case 0x8:
                    return "Cooperative blocking"; // worker thread count was adjusted up or down due to a worker thread synchronously waiting on a Task

                default:
                    return null;
            }
        }
    }

    public class ThreadPoolWorkerThreadAdjustmentSample : ThreadEvent
    {
        public double Throughput { get; }

        public ThreadPoolWorkerThreadAdjustmentSample(ReadOnlyCollection<object> payload) : base(EventType.ThreadPoolWorkerThreadAdjustmentSample)
        {
            Throughput = (double)payload[0];
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Throughput)] = Throughput;
            return json;
        }

        public override string ToString()
        {
            var str = base.ToString();
            return $"{str}, throughput: {Throughput}";
        }
    }

    public class ThreadPoolWorkerThreadAdjustmentStats : ThreadEvent
    {
        public double Duration { get; } // Amount of time, in seconds, during which these statistics were collected.
        public double Throughput { get; } // Average number of completions per second during this interval.
        public double ThroughputRatio { get; } // The relative improvement in throughput caused by variations in active worker thread count during this interval.
        public double Confidence { get; } // A measure of the validity of the ThroughputRatio field.
        public double NewControlSetting { get; } // The number of active worker threads that serve as the baseline for future variations in active thread count.
        public ushort NewThreadWaveMagnitude { get; } // The magnitude of future variations in active thread count.

        public ThreadPoolWorkerThreadAdjustmentStats(ReadOnlyCollection<object> payload) : base(EventType.ThreadPoolWorkerThreadAdjustmentStats)
        {
            Duration = (double)payload[0];
            Throughput = (double)payload[1];
            ThroughputRatio = (double)payload[6];
            Confidence = (double)payload[7];
            NewControlSetting = (double)payload[8];
            NewThreadWaveMagnitude = (ushort)payload[9];
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Duration)] = Duration;
            json[nameof(Throughput)] = Throughput;
            json[nameof(ThroughputRatio)] = ThroughputRatio;
            json[nameof(Confidence)] = Confidence;
            json[nameof(NewControlSetting)] = NewControlSetting;
            json[nameof(NewThreadWaveMagnitude)] = NewThreadWaveMagnitude;
            return json;
        }

        public override string ToString()
        {
            var str = base.ToString();
            return $"{str}, collected for: {Duration}ms, throughput: {Throughput}, throughput ratio: {ThroughputRatio}, confidence: {Confidence}, " +
                   $"new control setting: {NewControlSetting}, new thread wave magnitude: {NewThreadWaveMagnitude}";
        }
    }
}
