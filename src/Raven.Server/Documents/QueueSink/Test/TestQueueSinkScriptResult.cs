using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.QueueSink.Test
{
    public class TestQueueSinkScriptResult
    {
        public List<string> DebugOutput { get; set; }

        public DynamicJsonValue Actions { get; set; }
    }
}
