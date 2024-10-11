using System;
using System.Buffers;
using Raven.Client.Documents.Indexes.Vector;

namespace Raven.Server.Documents.Indexes.VectorSearch;

public struct VectorValue(ArrayPool<byte> arrayPool, byte[] buffer, Memory<byte> embedding) : IDisposable
{
    private byte[] _embedding = buffer;
    public Memory<byte> Embedding = embedding;
    
    //Get this via getter due to disposal risk, if we dispose then byte[] is null!
    public byte[] EmbeddingAsBytes => _embedding;

    public void Dispose()
    {
        if (_embedding == null) 
            return;
            
        arrayPool?.Return(_embedding, clearArray: true);
        _embedding = null;
        Embedding = null;
    }
}
