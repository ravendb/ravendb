using System;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingsMismatchException : Exception
{
    public EmbeddingsMismatchException(string message) : base(message)
    {
    }
}
