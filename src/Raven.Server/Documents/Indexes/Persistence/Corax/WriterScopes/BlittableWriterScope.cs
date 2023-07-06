using System;
using System.Buffers;
using System.IO;
using Corax;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;

public struct BlittableWriterScope : IDisposable
{
    private BlittableJsonReaderObject _reader;

    public BlittableWriterScope(BlittableJsonReaderObject reader)
    {
        _reader = reader;
    }

    public unsafe void Write(string path, int field, IndexWriter.IndexEntryBuilder writer)
    {
        if (_reader.HasParent == false)
        {
            writer.Store(field, path, new Span<byte>(_reader.BasePointer, _reader.Size));
        }
        else
        {
            using var clonedBlittable = _reader.CloneOnTheSameContext();
            writer.Store(field, path, new Span<byte>(clonedBlittable.BasePointer, clonedBlittable.Size));
        }
    }

    public void Dispose()
    {
    }
}
