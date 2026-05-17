using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Stats;

internal sealed class TaskErrorsResponse
{
    public string NodeTag { get; set; }
    public int? ShardNumber { get; set; }
    public List<TaskErrors> Results { get; set; } = new();
}
