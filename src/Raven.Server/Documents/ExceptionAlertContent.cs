using Raven.Server.Alerts;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class ExceptionAlertContent : IAlertContent
    {
        public string Message { get; set; }

        public string Exception { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Message)] = Message,
                [nameof(Exception)] = Exception
            };
        }
    }
}