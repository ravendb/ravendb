using System.Collections.Generic;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public class LicenseLimits
    {
        public Dictionary<string, int> CoresByNode { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(CoresByNode)] = TypeConverter.ToBlittableSupportedType(CoresByNode)
            };
        }
    }
}
