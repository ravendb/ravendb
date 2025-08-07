using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Test;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi.Test
{
    public sealed class TestGenAiScript : TestEtlScript<GenAiConfiguration, AiConnectionString>
    {
        public List<GenAiResultItem> Input { get; set; }

        public TestStage TestStage { get; set; }

        public BlittableJsonReaderObject Document { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(TestStage)] = TestStage;
            json[nameof(Document)] = Document;

            if (Input != null)
                json[nameof(Input)] = new DynamicJsonArray(Input.Select(x => x.ToJson()));

            return json;
        }
    }

    public enum TestStage
    {
        CreateContextObjects,
        SendToModel,
        ApplyUpdateScript
    }
}
