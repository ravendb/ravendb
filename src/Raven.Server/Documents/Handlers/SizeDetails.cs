using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class SizeDetails : IDynamicJson
    {
        public int ActualSize { get; set; }
        public string HumaneActualSize { get; set; }
        public int AllocatedSize { get; set; }
        public string HumaneAllocatedSize { get; set; }
        public bool IsCompressed { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ActualSize)] = ActualSize,
                [nameof(HumaneActualSize)] = HumaneActualSize,
                [nameof(AllocatedSize)] = AllocatedSize,
                [nameof(HumaneAllocatedSize)] = HumaneAllocatedSize,
                [nameof(IsCompressed)] = IsCompressed
            };
        }
    }
}
