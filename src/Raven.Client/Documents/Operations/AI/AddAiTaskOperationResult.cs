using Raven.Client.Documents.Operations.ETL;

namespace Raven.Client.Documents.Operations.AI
{
    /// <summary>
    /// Common result for AI ETL “add” operations.
    /// </summary>
    public abstract class AddAiTaskOperationResult : AddEtlOperationResult
    {
        public string Identifier { get; set; }
    }

    public sealed class AddGenAiOperationResult : AddAiTaskOperationResult
    {
    }

    public sealed class AddEmbeddingsGenerationOperationResult : AddAiTaskOperationResult
    {
    }
}
