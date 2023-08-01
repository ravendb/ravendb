using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public sealed class ExceptionDetails : INotificationDetails
    {
        public ExceptionDetails()
        {
        }

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
