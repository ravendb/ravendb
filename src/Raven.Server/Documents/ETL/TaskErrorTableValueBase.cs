using System;

namespace Raven.Server.Documents.ETL;

public abstract class TaskErrorTableValueBase
{
    public DateTime CreatedAt;
    public string TaskName;
    public long Step;
    public string Error;
}
