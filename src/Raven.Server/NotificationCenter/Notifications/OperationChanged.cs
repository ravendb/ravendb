using System;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Operations;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications
{
    public class OperationChanged : Notification
    {
        private OperationChanged() : base(NotificationType.OperationChanged)
        {
        }

        public override string Id => $"{Type}/{OperationId}";

        public long OperationId { get; private set; }

        public OperationState State { get; private set; }

        public bool Killable { get; private set; }

        public DateTime StartTime { get; private set; }

        public DateTime? EndTime { get; private set; }

        public DatabaseOperations.OperationType TaskType { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var result = base.ToJson();

            result[nameof(OperationId)] = OperationId;
            result[nameof(State)] = State.ToJson();
            result[nameof(Killable)] = Killable;
            result[nameof(TaskType)] = TaskType.ToString();
            result[nameof(StartTime)] = StartTime;
            result[nameof(EndTime)] = EndTime;

            return result;
        }

        public static OperationChanged Create(long id, DatabaseOperations.OperationDescription description, OperationState state, bool killable)
        {
            NotificationSeverity severity;

            switch (state.Status)
            {
                case OperationStatus.InProgress:
                    severity = NotificationSeverity.None;
                    break;
                case OperationStatus.Canceled:
                    severity = NotificationSeverity.Warning;
                    break;
                case OperationStatus.Completed:
                    severity = NotificationSeverity.Success;
                    break;
                case OperationStatus.Faulted:
                    severity = NotificationSeverity.Error;
                    break;
                default:
                    throw new ArgumentException($"Unknown operation status: {state.Status}");
            }

            return new OperationChanged
            {
                OperationId = id,
                State = state,
                Title = $"{description.TaskType.GetDescription()}",
                Message = description.Description,
                IsPersistent = state.Result?.ShouldPersist ?? false,
                Killable = killable && state.Status == OperationStatus.InProgress,
                Severity = severity,
                TaskType = description.TaskType,
                StartTime = description.StartTime,
                EndTime = description.EndTime
            };
        }
    }
}