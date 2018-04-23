using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class ExceptionDetails : INotificationDetails
    {
        public ExceptionDetails(Exception e)
        {
            Exception = e.ToString();
        }

        public string Exception { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Exception)] = Exception
            };
        }
    }
}
