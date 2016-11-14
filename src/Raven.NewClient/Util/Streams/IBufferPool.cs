using System;

namespace Raven.NewClient.Abstractions.Util.Streams
{
    public interface IBufferPool : IDisposable
    {
        byte[] TakeBuffer(int size);
        void ReturnBuffer(byte[] buffer);
    }
}
