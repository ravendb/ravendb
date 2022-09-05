using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Operations;

public abstract class AbstractOperation
{
    private readonly TimeSpan _throttleTime = TimeSpan.FromSeconds(1);

    private readonly ThrottledNotification _throttle = new();

    public long Id;

    public OperationDescription Description;

    public OperationState State;

    [JsonDeserializationIgnore]
    public OperationCancelToken Token;

    [JsonDeserializationIgnore]
    public Task<IOperationResult> Task;

    [JsonDeserializationIgnore]
    public string DatabaseName;

    public bool Killable => Token != null;

    public void SetCompleted()
    {
        Task = null;
    }

    public bool IsCompleted()
    {
        var task = Task;
        return task == null || task.IsCompleted;
    }

    public async Task KillAsync(bool waitForCompletion, CancellationToken token)
    {
        if (IsCompleted())
            return;

        if (Killable == false)
            throw new InvalidOperationException($"Operation {Id} is unkillable.");

        Token.Cancel();

        if (waitForCompletion == false)
            return;

        var task = Task;
        if (task != null)
            await task;
    }

    public void NotifyCenter(OperationChanged notification, Action<OperationChanged> addToNotificationCenter)
    {
        if (ShouldThrottleMessage(notification) == false)
        {
            addToNotificationCenter(notification);
            return;
        }

        // let us throttle changes about the operation progress

        var now = SystemTime.UtcNow;

        _throttle.Notification = notification;

        var sinceLastSent = now - _throttle.SentAt;

        if (_throttle.Scheduled == null && sinceLastSent > _throttleTime)
        {
            addToNotificationCenter(_throttle.Notification);
            _throttle.SentAt = now;

            return;
        }

        if (_throttle.Scheduled == null)
        {
            _throttle.Scheduled = System.Threading.Tasks.Task.Delay(_throttleTime - sinceLastSent).ContinueWith(x =>
            {
                if (State.Status == OperationStatus.InProgress)
                    addToNotificationCenter(_throttle.Notification);

                _throttle.SentAt = DateTime.UtcNow;
                _throttle.Scheduled = null;
            });
        }

        static bool ShouldThrottleMessage(OperationChanged notification)
        {
            if (notification.State.Status != OperationStatus.InProgress)
            {
                return false;
            }

            return true;
        }
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Id)] = Id,
            [nameof(Description)] = Description.ToJson(),
            [nameof(Killable)] = Killable,
            [nameof(State)] = State.ToJson()
        };
    }

    protected class ThrottledNotification
    {
        public OperationChanged Notification;

        public DateTime SentAt;

        public Task Scheduled;
    }
}
