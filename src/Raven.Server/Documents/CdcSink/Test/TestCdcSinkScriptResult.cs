using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.CdcSink.Test
{
    public class TestCdcSinkScriptResult : IDynamicJson
    {
        public List<string> DebugOutput { get; set; }

        public DynamicJsonValue Actions { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DebugOutput)] = new DynamicJsonArray(DebugOutput),
                [nameof(Actions)] = Actions
            };
        }
    }
}
