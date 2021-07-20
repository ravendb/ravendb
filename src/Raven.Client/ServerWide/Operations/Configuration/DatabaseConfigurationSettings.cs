using System.Collections.Generic;
using System.Linq;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class DatabaseConfigurationSettings : IDynamicJson
    {
        public Dictionary<string, string> Values { get; set; }
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Values)] = DynamicJsonValue.Convert(Values)
            };
        }
    }
}
