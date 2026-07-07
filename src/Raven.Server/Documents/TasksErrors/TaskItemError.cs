using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.TasksErrors;

public class TaskItemError : TaskErrorBase
{
    public string DocumentId { get; set; }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(DocumentId)] = DocumentId;

        return json;
    }
}
