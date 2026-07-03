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
