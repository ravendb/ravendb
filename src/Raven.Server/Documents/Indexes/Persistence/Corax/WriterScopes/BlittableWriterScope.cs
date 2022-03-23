using System;
using System.Buffers;
using System.IO;
using Corax;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;

public class BlittableWriterScope : IDisposable
{
    private readonly byte[] _buffer;
    private int _currentIndex;
    private BlittableJsonReaderObject _reader;

    public BlittableWriterScope(BlittableJsonReaderObject reader)
    {
        _currentIndex = 0;
        _reader = reader;
        _buffer = ArrayPool<byte>.Shared.Rent(_reader.Size);
    }

    public unsafe void Write(int field, ref IndexEntryWriter writer)
    {
        fixed (byte* ptr = _buffer)
        {
            if (_reader.HasParent == false)
            {
                writer.WriteRaw(field, new Span<byte>(_reader.BasePointer, _reader.Size));
                
            }
            else
            {
                using var clonedBlittable = _reader.CloneOnTheSameContext();
                writer.WriteRaw(field, new Span<byte>(clonedBlittable.BasePointer, clonedBlittable.Size));
            }
        }
    }

    
    
    public void Dispose()
    {
        ArrayPool<byte>.Shared.Return(_buffer);
    }
}
