using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Graph
{
    public class EdgeInfo : IDynamicJsonValueConvertible
    {        
        public string To { get; set; }

        public string EdgeType { get; set; }

        public Dictionary<string, string> Attributes { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            var dja = new DynamicJsonValue();
            if (Attributes != null)
            {
                foreach (var item in Attributes)
                {
                    dja[item.Key] = item.Value;
                }
            }

            var val = new DynamicJsonValue
            {
                [nameof(To)] = To,
                [nameof(EdgeType)] = EdgeType,
                [nameof(Attributes)] = dja
            };

            return val;
        }
    }
}
