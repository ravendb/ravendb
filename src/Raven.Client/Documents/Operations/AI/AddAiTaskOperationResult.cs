using Raven.Client.Documents.Operations.ETL;

namespace Raven.Client.Documents.Operations.AI
{
    /// <summary>
    /// Common result for AI ETL “add” operations.
    /// </summary>
    public abstract class AddAiTaskOperationResult : AddEtlOperationResult
    {
        /// <summary>
        /// The created task identifier.
        /// </summary>
        public string Identifier { get; set; }
    }

    /// <summary>
    /// Result returned by the server after adding a GenAI task.
    /// </summary>
    public sealed class AddGenAiOperationResult : AddAiTaskOperationResult
    {
    }

    /// <summary>
    /// Result returned by the server after adding an embeddings generation task.
    /// </summary>
    public sealed class AddEmbeddingsGenerationOperationResult : AddAiTaskOperationResult
    {
    }
}
