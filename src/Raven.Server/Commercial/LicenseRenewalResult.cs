using System;
using System.Collections.Generic;
using System.Text;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public class LicenseRenewalResult
    {
        public string Message { get; set; }
        public string Error { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Message)] = Message,
                [nameof(Error)] = Error
            };
        }
    }
}
