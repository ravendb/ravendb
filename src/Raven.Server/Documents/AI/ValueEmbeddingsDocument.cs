using System;
using JetBrains.Annotations;

namespace Raven.Server.Documents.AI;

public class ValueEmbeddingsDocument : IDisposable
{
    private readonly Document _inner;

    public ValueEmbeddingsDocument([NotNull] Document inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public string GetAttachmentNameForValue(string value)
    {
        return _inner.Data.TryGet(value, out string attachmentName) 
            ? attachmentName 
            : null;
    }

    public void Dispose()
    {
        _inner?.Dispose();
    }
}
