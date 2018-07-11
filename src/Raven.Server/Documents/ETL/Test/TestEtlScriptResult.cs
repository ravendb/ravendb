using System.Collections.Generic;
using Raven.Server.NotificationCenter.Notifications.Details;

namespace Raven.Server.Documents.ETL.Test
{
    public abstract class TestEtlScriptResult
    {
        public List<EtlErrorInfo> TransformationErrors { get; set; }

        public List<string> DebugOutput { get; set; }
    }
}
