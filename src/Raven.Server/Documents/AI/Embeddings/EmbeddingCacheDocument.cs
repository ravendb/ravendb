using System;
using JetBrains.Annotations;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingCacheDocument : IDisposable
{
    public readonly Document Inner;

    public EmbeddingCacheDocument([NotNull] Document inner)
    {
        Inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public void Dispose()
    {
        Inner?.Dispose();
    }
}
