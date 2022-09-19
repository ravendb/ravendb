using System.Collections.Generic;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Test
{
    public abstract class TestEtlScriptResult
    {
        public List<EtlErrorInfo> TransformationErrors { get; set; }

        public List<string> DebugOutput { get; set; }

        public virtual DynamicJsonValue ToJson(JsonOperationContext context)
        {
            return (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(this);
        }
    }
}
