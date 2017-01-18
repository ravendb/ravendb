using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions.Details
{
    public class MessageDetails : IActionDetails
    {
        public string Message { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Message)] = Message
            };
        }
    }
}