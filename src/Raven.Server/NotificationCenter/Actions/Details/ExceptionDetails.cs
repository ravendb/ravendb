using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions.Details
{
    public class ExceptionDetails : IActionDetails
    {
        public ExceptionDetails(Exception e)
        {
            Exception = e;
        }

        public Exception Exception { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Exception)] = Exception.ToString()
            };
        }
    }
}