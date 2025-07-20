using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.ETL.Test;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;

public class GenAiTestScriptResult : TestEtlScriptResult
{
    public BlittableJsonReaderObject ModifiedDocument;

    public BlittableJsonReaderObject OriginalDocument;

    public List<GenAiResultItem> Results;

    public BlittableJsonReaderObject InputDocument;

    public BlittableJsonReaderObject OutputDocument;

    public DynamicJsonValue DebugActions;

    public PatchStatus Status;

    public override DynamicJsonValue ToJson(JsonOperationContext context)
    {
        var json = base.ToJson(context);
        
        json[nameof(Status)] = Status;

        if (ModifiedDocument != null)
            json[nameof(ModifiedDocument)] = ModifiedDocument;
        if (OriginalDocument != null)
            json[nameof(OriginalDocument)] = OriginalDocument;

        json[nameof(OutputDocument)] = OutputDocument;
        json[nameof(InputDocument)] = InputDocument;

        if (Results != null)
            json[nameof(Results)] = new DynamicJsonArray(Results.Select(x => x.ToJson()));

        DynamicJsonValue dbg = null;

        if (DebugOutput?.Count > 0)
            dbg = new DynamicJsonValue { ["Output"] = new DynamicJsonArray(DebugOutput ?? []) };
        

        if (DebugActions != null)
            (dbg ??= new DynamicJsonValue())["Actions"] = DebugActions;
        
        json["Debug"] = dbg;

        return json;
    }
}
