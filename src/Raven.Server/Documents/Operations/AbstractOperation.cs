using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Operations;

public abstract class AbstractOperation
{
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

    public virtual async ValueTask KillAsync(bool waitForCompletion, CancellationToken token)
    {
        if (IsCompleted())
            return;

        if (Killable)
            Token.Cancel();

        if (waitForCompletion == false)
            return;

        var task = Task;
        if (task != null)
            await task;
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
