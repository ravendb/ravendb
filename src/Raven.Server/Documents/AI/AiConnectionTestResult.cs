using System.Collections.Generic;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI;

public sealed class AiConnectionTestResult : IDynamicJson
{
    public bool Success;

    public string Error;

    public List<LogEntry> Log;

    public bool AcceptsImageInput;

    public bool SupportsTools;

    public DynamicJsonValue ToJson()
    {
        var djv = new DynamicJsonValue
        {
            [nameof(Success)] = Success,
            [nameof(Error)] = Error,
            [nameof(AcceptsImageInput)] = AcceptsImageInput,
            [nameof(SupportsTools)] = SupportsTools
        };

        if (Log != null)
            djv[nameof(Log)] = new DynamicJsonArray(collection: Log);

        return djv;
    }
}
