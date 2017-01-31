using Raven.Client.Data;
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

        public override DynamicJsonValue ToJson()
        {
            var result = base.ToJson();

            result[nameof(OperationId)] = OperationId;
            result[nameof(State)] = State.ToJson();
            result[nameof(Killable)] = Killable;

            return result;
        }

        public static OperationChanged Create(long id, DatabaseOperations.OperationDescription description, OperationState state, bool killable)
        {
            return new OperationChanged
            {
                OperationId = id,
                State = state,
                Title = $"{description.TaskType.GetDescription()}",
                Message = description.Description,
                IsPersistent = state.Result?.ShouldPersist ?? false,
                Killable = killable && state.Status == OperationStatus.InProgress
            };
        }
    }
}