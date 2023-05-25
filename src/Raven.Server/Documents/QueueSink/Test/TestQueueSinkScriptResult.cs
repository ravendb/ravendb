using System.Collections.Generic;
using Raven.Server.NotificationCenter.Notifications.Details;

namespace Raven.Server.Documents.QueueSink.Test
{
    public class TestQueueSinkScriptResult
    {
        public List<QueueSinkErrorInfo> ScriptErrors { get; set; }

        public List<string> DebugOutput { get; set; }
    }
}
