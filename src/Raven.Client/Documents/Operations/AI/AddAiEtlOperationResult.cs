using Raven.Client.Documents.Operations.ETL;

namespace Raven.Client.Documents.Operations.AI
{
    /// <summary>
    /// Common result for AI ETL “add” operations.
    /// </summary>
    public abstract class AddAiEtlOperationResult : AddEtlOperationResult
    {
        public string Identifier { get; set; }
    }

    public sealed class AddGenAiOperationResult : AddAiEtlOperationResult
    {
    }

    public sealed class AddEmbeddingsGenerationOperationResult : AddAiEtlOperationResult
    {
    }
}
