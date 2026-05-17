using Raven.Client.Documents.Operations.ETL;

namespace Raven.Server.Documents.ETL;

public enum TaskCategory
{
    Etl,
    Ai
}

public static class TaskTypeExtensions
{
    public static TaskCategory FromEtlType(EtlType etlType)
    {
        return etlType is EtlType.EmbeddingsGeneration or EtlType.GenAi
            ? TaskCategory.Ai
            : TaskCategory.Etl;
    }
}
