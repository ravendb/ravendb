using System;

namespace Raven.Client.Util.Streams
{
    public interface IBufferPool : IDisposable
    {
        byte[] TakeBuffer(int size);
        void ReturnBuffer(byte[] buffer);
    }
}
