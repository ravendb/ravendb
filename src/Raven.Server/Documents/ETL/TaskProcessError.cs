using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL;

public class TaskProcessError : TaskErrorBase
{
    public long AffectedDocumentsCount { get; set; }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(AffectedDocumentsCount)] = AffectedDocumentsCount;

        return json;
    }
}
