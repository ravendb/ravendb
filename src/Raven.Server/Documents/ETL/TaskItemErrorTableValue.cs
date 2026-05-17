namespace Raven.Server.Documents.ETL;

public class TaskItemErrorTableValue : TaskErrorTableValueBase
{
    public string DocumentId;

    public TaskItemError ToTaskItemError()
    {
        return new TaskItemError
        {
            CreatedAt = CreatedAt,
            TaskName = TaskName,
            DocumentId = DocumentId,
            Step = (TaskErrorStep)Step,
            Error = Error
        };
    }
}
