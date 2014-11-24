using System;

namespace Raven.Abstractions.Util.Streams
{
    public interface IBufferPool : IDisposable
    {
        byte[] TakeBuffer(int size);
        void ReturnBuffer(byte[] buffer);
    }
}