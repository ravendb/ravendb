using System;
using Raven.Client.Documents.Operations.ETL;

namespace Raven.Server.Documents.TasksErrors;

public enum TaskCategory
{
    Etl,
    Ai,
    CdcSink
}

public static class TaskTypeExtensions
{
    public static TaskCategory FromEtlType(EtlType etlType)
    {
        return etlType is EtlType.EmbeddingsGeneration or EtlType.GenAi
            ? TaskCategory.Ai
            : TaskCategory.Etl;
    }

    // Single source of truth for the per-category server route used to read/delete task errors, so the
    // client commands can't drift apart or miss an arm when a new task category is added.
    public static string ErrorsEndpoint(this TaskCategory category)
    {
        return category switch
        {
            TaskCategory.Etl => "etl/errors",
            TaskCategory.Ai => "ai/errors",
            TaskCategory.CdcSink => "cdc-sink/errors",
            _ => throw new ArgumentOutOfRangeException(nameof(category), category, null)
        };
    }
}
