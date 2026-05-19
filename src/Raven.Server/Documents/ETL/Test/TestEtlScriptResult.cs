using System.Collections.Generic;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Test
{
    public abstract class TestEtlScriptResult
    {
        public List<TaskItemError> TransformationErrors { get; set; }

        public List<string> DebugOutput { get; set; }

        public virtual DynamicJsonValue ToJson(JsonOperationContext context)
        {
            return (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(this);
        }
    }
}
