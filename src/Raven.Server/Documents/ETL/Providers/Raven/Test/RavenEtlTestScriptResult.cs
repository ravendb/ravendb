using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Server.Documents.ETL.Test;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Raven.Test
{
    public class RavenEtlTestScriptResult : TestEtlScriptResult
    {
        public List<ICommandData> Commands { get; set; }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = new DynamicJsonValue
            {
                [nameof(TransformationErrors)] = new DynamicJsonArray(TransformationErrors.Select(x => x.ToJson())),
                [nameof(DebugOutput)] = new DynamicJsonArray(DebugOutput)
            };

            if (Commands != null)
                json[nameof(Commands)] = new DynamicJsonArray(Commands.Select(x => x.ToJson(DocumentConventions.DefaultForServer, context)));

            return json;
        }
    }
}
