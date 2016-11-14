using Sparrow.Json.Parsing;

namespace Raven.NewClient.Client.Data
{
    public class OperationStatusChangeNotification : Notification
    {
        public long OperationId { get; set; }

        public OperationState State { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                ["OperationId"] = OperationId,
                ["State"] = State.ToJson()
            };
        }
    }
}