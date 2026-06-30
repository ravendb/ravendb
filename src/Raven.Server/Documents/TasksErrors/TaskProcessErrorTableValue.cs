namespace Raven.Server.Documents.ETL;

public class TaskProcessErrorTableValue : TaskErrorTableValueBase
{
    public long AffectedDocumentsCount;

    public TaskProcessError ToTaskProcessError()
    {
        return new TaskProcessError
        {
            CreatedAt = CreatedAt,
            TaskName = TaskName,
            AffectedDocumentsCount = AffectedDocumentsCount,
            Step = (TaskErrorStep)Step,
            Error = Error
        };
    }
}
