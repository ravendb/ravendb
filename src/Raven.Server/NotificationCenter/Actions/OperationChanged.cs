using Raven.Client.Data;
using Raven.Server.Documents;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions
{
    public class OperationChanged : Action
    {
        private OperationChanged()
        {
        }

        public long OperationId { get; set; }

        public OperationState State { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var result = base.ToJson();

            result[nameof(OperationId)] = OperationId;
            result[nameof(State)] = State.ToJson();

            return result;
        }

        public static OperationChanged Create(long id, DatabaseOperations.OperationDescription description, OperationState state)
        {
            return new OperationChanged
            {
                OperationId = id,
                State = state,
                Title = $"{description.TaskType.GetDescription()}",
                Message = description.Description,
                Type = ActionType.Operation,
                IsPersistent = false,
            };
        }
    }
}