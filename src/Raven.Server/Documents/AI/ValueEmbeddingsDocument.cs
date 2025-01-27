using System;
using JetBrains.Annotations;

namespace Raven.Server.Documents.AI;

public class ValueEmbeddingsDocument : IDisposable
{
    public readonly Document Inner;

    public ValueEmbeddingsDocument([NotNull] Document inner)
    {
        Inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public string GetAttachmentNameForValue(string value)
    {
        return Inner.Data.TryGet(value, out string attachmentName) 
            ? attachmentName 
            : null;
    }

    public void Dispose()
    {
        Inner?.Dispose();
    }
}
