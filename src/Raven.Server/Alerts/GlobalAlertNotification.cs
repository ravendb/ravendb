using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Data;
using Sparrow.Json.Parsing;

namespace Raven.Server.Alerts
{
    //TODO consolidate with Operations.AlertNotification
    public class GlobalAlertNotification : Notification
    {
        public string Operation { get; set; }

        public string Id { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Operation)] = Operation,
                [nameof(Id)] = Id
            };
        }
    }
}
